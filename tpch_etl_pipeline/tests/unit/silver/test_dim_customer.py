import pytest
from unittest.mock import patch, MagicMock
from tpch_etl_pipeline.etl.silver.dim_customer import DimCustomerSilverETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet

def test_dim_customer_transform(spark, sample_customer_data, sample_nation_data, sample_region_data):
    """Test la transformation de la couche silver pour dim_customer."""
    # Créer une instance de DimCustomerSilverETL avec un mock pour extract_upstream
    with patch.object(DimCustomerSilverETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        customer_dataset = ETLDataSet(
            name='customer',
            curr_data=sample_customer_data,
            primary_keys=['c_custkey'],
            storage_path='s3a://spark-bucket/delta/bronze/customer',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        nation_dataset = ETLDataSet(
            name='nation',
            curr_data=sample_nation_data,
            primary_keys=['n_nationkey'],
            storage_path='s3a://spark-bucket/delta/bronze/nation',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        region_dataset = ETLDataSet(
            name='region',
            curr_data=sample_region_data,
            primary_keys=['r_regionkey'],
            storage_path='s3a://spark-bucket/delta/bronze/region',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        mock_extract.return_value = [customer_dataset, nation_dataset, region_dataset]
        
        # Créer l'instance ETL avec load_data=False
        dim_customer_etl = DimCustomerSilverETL(spark=spark, load_data=False)
        
        # Exécuter la transformation
        result = dim_customer_etl.transform_upstream(dim_customer_etl.extract_upstream())
        
        # Vérifier que le résultat est correct
        assert result.name == 'dim_customer'
        assert result.primary_keys == ['customer_key']
        
        # Vérifier les colonnes attendues
        expected_columns = [
            'customer_key', 'customer_name', 'street_address', 
            'nation_name', 'region_name', 'full_address', 
            'phone_number', 'account_balance', 'market_segment', 'etl_inserted'
        ]
        
        for col_name in expected_columns:
            assert col_name in result.curr_data.columns
        
        # Vérifier les jointures
        # Les clients avec nationkey=1 devraient avoir nation_name='FRANCE' et region_name='EUROPE'
        france_customers = result.curr_data.filter("nation_name = 'FRANCE'").collect()
        assert len(france_customers) > 0
        for customer in france_customers:
            assert customer['region_name'] == 'EUROPE'
        
        # Vérifier le nombre de lignes
        assert result.curr_data.count() == sample_customer_data.count()

def test_dim_customer_read(spark, sample_customer_data, sample_nation_data, sample_region_data):
    """Test la lecture des données de la couche silver pour dim_customer."""
    # Créer une instance de DimCustomerSilverETL avec un mock pour extract_upstream
    with patch.object(DimCustomerSilverETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        customer_dataset = ETLDataSet(
            name='customer',
            curr_data=sample_customer_data,
            primary_keys=['c_custkey'],
            storage_path='s3a://spark-bucket/delta/bronze/customer',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        nation_dataset = ETLDataSet(
            name='nation',
            curr_data=sample_nation_data,
            primary_keys=['n_nationkey'],
            storage_path='s3a://spark-bucket/delta/bronze/nation',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        region_dataset = ETLDataSet(
            name='region',
            curr_data=sample_region_data,
            primary_keys=['r_regionkey'],
            storage_path='s3a://spark-bucket/delta/bronze/region',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        mock_extract.return_value = [customer_dataset, nation_dataset, region_dataset]
        
        # Créer l'instance ETL avec load_data=False
        dim_customer_etl = DimCustomerSilverETL(spark=spark, load_data=False)
        
        # Exécuter la transformation pour initialiser les données
        dim_customer_etl.transform_upstream(dim_customer_etl.extract_upstream())
        
        # Tester la méthode read
        result = dim_customer_etl.read()
        
        # Vérifier que le résultat est correct
        assert result.name == 'dim_customer'
        assert result.primary_keys == ['customer_key']
        
        # Vérifier les colonnes attendues
        expected_columns = [
            'customer_key', 'customer_name', 'street_address', 
            'nation_name', 'region_name', 'phone_number', 
            'account_balance', 'market_segment', 'etl_inserted'
        ]
        
        for col_name in expected_columns:
            assert col_name in result.curr_data.columns
