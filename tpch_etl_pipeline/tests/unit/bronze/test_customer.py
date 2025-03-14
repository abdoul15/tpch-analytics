import pytest
from pyspark.sql.functions import col
from unittest.mock import patch, MagicMock
from tpch_etl_pipeline.etl.bronze.customer import CustomerBronzeETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet

def test_customer_bronze_transform(spark, sample_customer_data):
    """Test la transformation de la couche bronze pour customer."""
    # Créer une instance de CustomerBronzeETL avec un mock pour extract_upstream
    with patch.object(CustomerBronzeETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        etl_dataset = ETLDataSet(
            name='customer',
            curr_data=sample_customer_data,
            primary_keys=['c_custkey'],
            storage_path='s3a://spark-bucket/delta/bronze/customer',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        mock_extract.return_value = [etl_dataset]
        
        # Créer l'instance ETL avec load_data=False pour éviter d'écrire sur le disque
        customer_etl = CustomerBronzeETL(spark=spark, load_data=False)
        
        # Exécuter la transformation
        result = customer_etl.transform_upstream(customer_etl.extract_upstream())
        
        # Vérifier que le résultat est correct
        assert result.name == 'customer'
        assert result.primary_keys == ['c_custkey']
        
        # Vérifier que la colonne etl_inserted a été ajoutée
        assert 'etl_inserted' in result.curr_data.columns
        
        # Vérifier que toutes les colonnes d'origine sont présentes
        for col_name in sample_customer_data.columns:
            assert col_name in result.curr_data.columns
        
        # Vérifier que le nombre de lignes est identique
        assert result.curr_data.count() == sample_customer_data.count()

def test_customer_bronze_read(spark, sample_customer_data):
    """Test la lecture des données de la couche bronze pour customer."""
    # Créer une instance de CustomerBronzeETL avec un mock pour read
    with patch.object(CustomerBronzeETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        etl_dataset = ETLDataSet(
            name='customer',
            curr_data=sample_customer_data,
            primary_keys=['c_custkey'],
            storage_path='s3a://spark-bucket/delta/bronze/customer',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        mock_extract.return_value = [etl_dataset]
        
        # Créer l'instance ETL avec load_data=False
        customer_etl = CustomerBronzeETL(spark=spark, load_data=False)
        
        # Exécuter la transformation pour initialiser les données
        customer_etl.transform_upstream(customer_etl.extract_upstream())
        
        # Tester la méthode read
        result = customer_etl.read()
        
        # Vérifier que le résultat est correct
        assert result.name == 'customer'
        assert result.primary_keys == ['c_custkey']
        
        # Vérifier que toutes les colonnes attendues sont présentes
        expected_columns = [
            'c_custkey', 'c_name', 'c_address', 'c_nationkey', 
            'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment', 'etl_inserted'
        ]
        
        for col_name in expected_columns:
            assert col_name in result.curr_data.columns
