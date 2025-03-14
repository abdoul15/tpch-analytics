import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import patch

from tpch_etl_pipeline.etl.bronze.customer import CustomerBronzeETL
from tpch_etl_pipeline.etl.bronze.nation import NationBronzeETL
from tpch_etl_pipeline.etl.bronze.region import RegionBronzeETL
from tpch_etl_pipeline.etl.silver.dim_customer import DimCustomerSilverETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet

def test_bronze_to_silver_integration(spark, sample_customer_data, sample_nation_data, 
                                     sample_region_data, temp_storage_dir):
    """Test d'intégration de la couche bronze à silver pour customer."""
    # Définir les chemins de stockage temporaires
    customer_path = f"{temp_storage_dir}/bronze/customer"
    nation_path = f"{temp_storage_dir}/bronze/nation"
    region_path = f"{temp_storage_dir}/bronze/region"
    dim_customer_path = f"{temp_storage_dir}/silver/dim_customer"
    
    # Créer et exécuter les ETL de la couche bronze avec des mocks pour l'extraction
    with pytest.MonkeyPatch.context() as mp:
        # Remplacer la méthode extract_upstream pour utiliser nos données de test
        def mock_customer_extract(self):
            return [ETLDataSet(
                name='customer',
                curr_data=sample_customer_data,
                primary_keys=['c_custkey'],
                storage_path=customer_path,
                data_format='delta',
                database='tpchdb',
                partition_keys=['etl_inserted']
            )]
        
        def mock_nation_extract(self):
            return [ETLDataSet(
                name='nation',
                curr_data=sample_nation_data,
                primary_keys=['n_nationkey'],
                storage_path=nation_path,
                data_format='delta',
                database='tpchdb',
                partition_keys=['etl_inserted']
            )]
        
        def mock_region_extract(self):
            return [ETLDataSet(
                name='region',
                curr_data=sample_region_data,
                primary_keys=['r_regionkey'],
                storage_path=region_path,
                data_format='delta',
                database='tpchdb',
                partition_keys=['etl_inserted']
            )]
        
        # Appliquer les mocks
        mp.setattr(CustomerBronzeETL, 'extract_upstream', mock_customer_extract)
        mp.setattr(NationBronzeETL, 'extract_upstream', mock_nation_extract)
        mp.setattr(RegionBronzeETL, 'extract_upstream', mock_region_extract)
        
        # Exécuter les ETL de la couche bronze
        customer_bronze = CustomerBronzeETL(
            spark=spark, 
            storage_path=customer_path,
            run_upstream=False
        )
        customer_bronze.run()
        
        nation_bronze = NationBronzeETL(
            spark=spark, 
            storage_path=nation_path,
            run_upstream=False
        )
        nation_bronze.run()
        
        region_bronze = RegionBronzeETL(
            spark=spark, 
            storage_path=region_path,
            run_upstream=False
        )
        region_bronze.run()
        
        # Créer une classe wrapper pour les tables en amont
        class CustomerBronzeWrapper(CustomerBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=customer_path, **kwargs)
        
        class NationBronzeWrapper(NationBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=nation_path, **kwargs)
        
        class RegionBronzeWrapper(RegionBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=region_path, **kwargs)
        
        # Exécuter l'ETL de la couche silver
        dim_customer = DimCustomerSilverETL(
            spark=spark,
            upstream_table_names=[
                CustomerBronzeWrapper,
                NationBronzeWrapper,
                RegionBronzeWrapper
            ],
            storage_path=dim_customer_path,
            run_upstream=False
        )
        dim_customer.run()
        
        # Vérifier que les données ont été correctement transformées
        dim_customer_data = spark.read.format('delta').load(dim_customer_path)
        
        # Vérifier le nombre de lignes
        assert dim_customer_data.count() == sample_customer_data.count()
        
        # Vérifier les colonnes
        expected_columns = [
            'customer_key', 'customer_name', 'street_address', 
            'nation_name', 'region_name', 'full_address', 
            'phone_number', 'account_balance', 'market_segment', 'etl_inserted'
        ]
        
        for col_name in expected_columns:
            assert col_name in dim_customer_data.columns
        
        # Vérifier les jointures
        france_customers = dim_customer_data.filter("nation_name = 'FRANCE'").collect()
        assert len(france_customers) > 0
        for customer in france_customers:
            assert customer['region_name'] == 'EUROPE'
