from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


from tpch_etl_pipeline.utils.etl_base import TableETL,ETLDataSet
from tpch_etl_pipeline.utils.database import get_table_from_db
from tpch_etl_pipeline.config.tables_config import TABLE_PARTITION_CONFIG



class PartSuppBronzeETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = None,
        name: str = "partsupp",
        primary_keys: List[str] = ["ps_partkey", "ps_suppkey"],
        storage_path: str = "s3a://spark-bucket/delta/bronze/partsupp",
        data_format: str = "delta",
        database: str = "tpchdb",
        partition_keys: List[str] = ["etl_inserted"],
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
        # Assuming partsupp data is extracted from a database or other source
        table_name = "public.partsupp"
        config = TABLE_PARTITION_CONFIG.get(table_name)
        partsupp_data = get_table_from_db(table_name, self.spark,partition_column=config['partition_column'])

        
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=partsupp_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return [etl_dataset]

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        partsupp_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Perform any necessary transformations on the partsupp data
        transformed_data = partsupp_data.withColumn(
            "etl_inserted", lit(current_timestamp)
        )

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(
        self, partition_values: Optional[Dict[str, str]] = None
    ) -> ETLDataSet:
        print(f"Lecture des données dans PartSupp (Bronze) démarré")
        
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
            partition_filter = " AND ".join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            # Optimisation: Utiliser l'API DeltaTable pour obtenir la dernière version
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(self.spark, self.storage_path)
                
                # Obtenir la dernière version de la table sans collect()
                # Utiliser une vue temporaire pour éviter collect()
                delta_table.history(1).select("version").createOrReplaceTempView("latest_version")
                latest_version = self.spark.sql("SELECT version FROM latest_version").first()[0]
                
                # Lire directement la dernière version sans filtrer
                partsupp_data = (
                    self.spark.read.format(self.data_format)
                    .option("versionAsOf", latest_version)
                    .load(self.storage_path)
                )
                
                # Explicitly select columns
                partsupp_data = partsupp_data.select(
                    col("ps_partkey"),
                    col("ps_suppkey"),
                    col("ps_availqty"),
                    col("ps_supplycost"),
                    col("ps_comment"),
                    col("etl_inserted"),
                )
                
                # Créer l'ETLDataSet et retourner
                etl_dataset = ETLDataSet(
                    name=self.name,
                    curr_data=partsupp_data,
                    primary_keys=self.primary_keys,
                    storage_path=self.storage_path,
                    data_format=self.data_format,
                    database=self.database,
                    partition_keys=self.partition_keys,
                )
                
                print(f"Lecture des données dans PartSupp (Bronze) Ok")
                return etl_dataset
                
            except Exception as e:
                # Fallback à la méthode originale si l'approche Delta échoue
                print(f"Optimisation de lecture échouée, utilisation de la méthode standard: {str(e)}")
                
                # Utiliser une vue temporaire pour éviter collect()
                self.spark.read.format(self.data_format) \
                    .load(self.storage_path) \
                    .selectExpr('max(etl_inserted) as max_etl_inserted') \
                    .createOrReplaceTempView("latest_partition")
                
                latest_partition = self.spark.sql("SELECT max_etl_inserted FROM latest_partition").first()[0]
                partition_filter = f"etl_inserted = '{latest_partition}'"

        # Read the partsupp data from the Delta Lake table
        partsupp_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        # Explicitly select columns
        partsupp_data = partsupp_data.select(
            col("ps_partkey"),
            col("ps_suppkey"),
            col("ps_availqty"),
            col("ps_supplycost"),
            col("ps_comment"),
            col("etl_inserted"),
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=partsupp_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
