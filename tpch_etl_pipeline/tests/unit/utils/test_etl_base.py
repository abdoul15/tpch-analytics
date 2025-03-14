import pytest
from unittest.mock import patch, MagicMock
import tempfile
import shutil
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from tpch_etl_pipeline.utils.etl_base import TableETL, ETLDataSet

class SimpleTestETL(TableETL):
    """Classe ETL simple pour tester la classe de base TableETL."""
    
    def __init__(
        self,
        spark,
        upstream_table_names=None,
        name='test_table',
        primary_keys=['id'],
        storage_path='/tmp/test_table',
        data_format='delta',
        database='test_db',
        partition_keys=['etl_inserted'],
        run_upstream=True,
        load_data=True,
    ):
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )
        self.curr_data = None
    
    def extract_upstream(self):
        # Créer un DataFrame simple pour les tests
        data = [
            (1, 'Test 1'),
            (2, 'Test 2'),
            (3, 'Test 3'),
        ]
        df = self.spark.createDataFrame(data, ['id', 'name'])
        
        return [
            ETLDataSet(
                name=self.name,
                curr_data=df,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        ]
    
    def transform_upstream(self, upstream_datasets):
        # Ajouter une colonne etl_inserted
        df = upstream_datasets[0].curr_data
        transformed_df = df.withColumn('etl_inserted', lit('2023-01-01'))
        
        self.curr_data = transformed_df
        
        return ETLDataSet(
            name=self.name,
            curr_data=transformed_df,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )
    
    def read(self, partition_values=None):
        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        else:
            # Simuler la lecture depuis le stockage
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

def test_table_etl_run(spark):
    """Test le flux complet de la classe TableETL."""
    # Créer un répertoire temporaire pour les tests
    temp_dir = tempfile.mkdtemp()
    try:
        # Créer une instance de SimpleTestETL avec load_data=False
        test_etl = SimpleTestETL(
            spark=spark,
            storage_path=os.path.join(temp_dir, 'test_table'),
                load_data=True  # Doit être True pour appeler load()
        )
        
        # Mocker la méthode load pour éviter d'écrire sur le disque
        with patch.object(test_etl, 'load') as mock_load:
            # Exécuter le flux ETL
            test_etl.run()
            
            # Vérifier que la méthode load a été appelée
            mock_load.assert_called_once()
            
            # Vérifier que les données ont été transformées
            args, _ = mock_load.call_args
            etl_dataset = args[0]
            
            assert etl_dataset.name == 'test_table'
            assert etl_dataset.primary_keys == ['id']
            assert 'etl_inserted' in etl_dataset.curr_data.columns
            assert etl_dataset.curr_data.count() == 3
    
    finally:
        # Nettoyer le répertoire temporaire
        shutil.rmtree(temp_dir)

def test_etl_dataset_creation():
    """Test la création d'une instance ETLDataSet."""
    # Créer un mock DataFrame
    mock_df = MagicMock(spec=DataFrame)
    
    # Créer une instance ETLDataSet
    etl_dataset = ETLDataSet(
        name='test_dataset',
        curr_data=mock_df,
        primary_keys=['id'],
        storage_path='/tmp/test_dataset',
        data_format='delta',
        database='test_db',
        partition_keys=['etl_inserted']
    )
    
    # Vérifier les attributs
    assert etl_dataset.name == 'test_dataset'
    assert etl_dataset.curr_data == mock_df
    assert etl_dataset.primary_keys == ['id']
    assert etl_dataset.storage_path == '/tmp/test_dataset'
    assert etl_dataset.data_format == 'delta'
    assert etl_dataset.database == 'test_db'
    assert etl_dataset.partition_keys == ['etl_inserted']
