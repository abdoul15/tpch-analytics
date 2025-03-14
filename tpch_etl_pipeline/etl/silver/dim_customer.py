from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws,broadcast


from tpch_etl_pipeline.etl.bronze.customer import CustomerBronzeETL
from tpch_etl_pipeline.etl.bronze.nation import NationBronzeETL
from tpch_etl_pipeline.etl.bronze.region import RegionBronzeETL
from tpch_etl_pipeline.utils.etl_base import TableETL, ETLDataSet


class DimCustomerSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            CustomerBronzeETL,
            NationBronzeETL,
            RegionBronzeETL,
        ],
        name: str = 'dim_customer',
        primary_keys: List[str] = ['customer_key'],
        storage_path: str = 's3a://spark-bucket/delta/silver/dim_customer',
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
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        customer_data = upstream_datasets[0].curr_data
        nation_data = upstream_datasets[1].curr_data
        region_data = upstream_datasets[2].curr_data
        current_timestamp = datetime.now()

            # Enrichir les données géographiques
        geo_data = (
            nation_data.join(
                broadcast(region_data),
                nation_data['n_regionkey'] == region_data['r_regionkey'],
                'left'
            )
            .select(
                col('n_nationkey'),
                col('n_name').alias('nation_name'),
                col('r_name').alias('region_name')  # Ajout de la colonne region_name
            )
        )

        # Joindre avec les données client
        transformed_data = (
            customer_data.join(
                broadcast(geo_data),
                customer_data['c_nationkey'] == geo_data['n_nationkey'],
                'left'
            )
            .select(
                # Informations client
                col('c_custkey').alias('customer_key'),
                col('c_name').alias('customer_name'),
                col('c_address').alias('street_address'),
                # Informations géographiques
                col('nation_name'),
                col('region_name'),  # Utilisation de la colonne region_name
                # Adresse complète
                concat_ws(', ', 
                    col('c_address'),
                    col('nation_name'),
                    col('region_name')
                ).alias('full_address'),
                # Autres informations
                col('c_phone').alias('phone_number'),
                col('c_acctbal').alias('account_balance'),
                col('c_mktsegment').alias('market_segment'),
                # Timestamp ETL
                lit(current_timestamp).alias('etl_inserted')
            )
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
        # Select the desired columns
        selected_columns = [
            col("customer_key"),
            col("customer_name"),
            col("street_address"),
            col('nation_name'),
            col('region_name'),
            col("phone_number"),
            col("account_balance"),
            col("market_segment"),
            col("etl_inserted")
        ]

        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data.select(selected_columns),
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
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(etl_inserted)")
                .collect()[0][0]
            )
            partition_filter = f"etl_inserted = '{latest_partition}'"

        # Read the customer dimension data
        dim_customer_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        dim_customer_data = dim_customer_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=dim_customer_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset