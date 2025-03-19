from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from tpch_etl_pipeline.utils import ETLDataSet, TableETL
from tpch_etl_pipeline.utils.database import get_table_from_db
from tpch_etl_pipeline.config.tables_config import TABLE_PARTITION_CONFIG


class CustomerBronzeETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = None,
        name: str = 'customer',
        primary_keys: List[str] = ['c_custkey'],
        storage_path: str = 's3a://spark-bucket/delta/bronze/customer',
        data_format: str = 'delta',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
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

    def extract_upstream(self) -> List[ETLDataSet]:
        # Extract customer data from TPCH source
        table_name = 'public.customer'

        config = TABLE_PARTITION_CONFIG.get(table_name)
        customer_data = get_table_from_db(
            table_name, self.spark, partition_column=config['partition_column']
        )

        # Creation d'une instance ETL
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=customer_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return [etl_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        # Mettre en cache le DataFrame pour éviter les recalculs
        customer_data = upstream_datasets[0].curr_data.cache()
        current_timestamp = datetime.now()

        # Notre transformation d'ajout d'une nouvelle colonne
        transformed_data = customer_data.withColumn(
            'etl_inserted', lit(current_timestamp)
        )

        # Libérer la mémoire après utilisation
        customer_data.unpersist()

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = transformed_data

        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
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

        elif partition_values:
            # Si des valeurs de partition spécifiques sont fournies, les utiliser
            partition_filter = ' AND '.join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            # Sinon, trouver la dernière partition etl_inserted
            # Cette approche bénéficie de l'élagage de partition (partition pruning)
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr('max(etl_inserted) as max_etl_inserted')
                .first()[0]
            )

            partition_filter = f"etl_inserted = '{latest_partition}'"
        # Lire les données avec le filtre de partition
        user_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        # Sélectionner explicitement les colonnes
        user_data = user_data.select(
            col('c_custkey'),
            col('c_name'),
            col('c_address'),
            col('c_nationkey'),
            col('c_phone'),
            col('c_acctbal'),
            col('c_mktsegment'),
            col('c_comment'),
            col('etl_inserted'),
        )

        # Créer une instance ETLDataSet
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=user_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
