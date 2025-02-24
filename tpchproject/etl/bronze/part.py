from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from tpchproject.utils import ETLDataSet, TableETL
from tpchproject.utils.database import get_table_from_db


class PartBronzeETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = None,
        name: str = 'part',
        primary_keys: List[str] = ['p_partkey'],
        storage_path: str = 's3a://spark-bucket/delta/bronze/part',
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
        # Extract part data from TPCH source
        table_name = 'public.part'
        part_data = get_table_from_db(table_name, self.spark)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=part_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return [etl_dataset]

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        part_data = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Add ETL timestamp
        transformed_data = part_data.withColumn('etl_inserted', lit(current_timestamp))

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
            partition_filter = ' AND '.join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr('max(etl_inserted)')
                .collect()[0][0]
            )
            partition_filter = f"etl_inserted = '{latest_partition}'"

        # Read the part data from the Delta Lake table
        part_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        # Explicitly select columns based on TPCH schema
        part_data = part_data.select(
            col('p_partkey'),  # Part key (Primary Key)
            col('p_name'),  # Part name
            col('p_mfgr'),  # Manufacturer
            col('p_brand'),  # Brand
            col('p_type'),  # Type
            col('p_size'),  # Size
            col('p_container'),  # Container
            col('p_retailprice'),  # Retail price
            col('p_comment'),  # Comment
            col('etl_inserted'),
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=part_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
