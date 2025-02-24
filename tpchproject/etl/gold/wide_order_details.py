from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from tpchproject.etl.silver.fct_orders import FctOrdersSilverETL
from tpchproject.etl.silver.dim_customer import DimCustomerSilverETL
from tpchproject.etl.silver.dim_part import DimPartSilverETL
from tpchproject.utils.base_table import ETLDataSet,TableETL



class WideOrderDetailsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            FctOrdersSilverETL,
            DimCustomerSilverETL,
            DimPartSilverETL
        ],
        name: str = "wide_order_details",
        primary_keys: List[str] = ["order_key", "line_number"],
        storage_path: str = "s3a://spark-bucket/delta/gold/wide_order_details",
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

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        orders_data = upstream_datasets[0].curr_data
        customer_data = upstream_datasets[1].curr_data
        part_data = upstream_datasets[2].curr_data
        current_timestamp = datetime.now()

        # Joindre les données de commande avec client et produit
        wide_orders_data = (
            orders_data
            .join(
                customer_data,
                orders_data["customer_key"] == customer_data["customer_key"],
                "left"
            )
            .join(
                part_data,
                [orders_data["part_key"] == part_data["part_key"],
                 orders_data["supplier_key"] == part_data["supplier_key"]],
                "left"
            )
        )

        # Supprimer les colonnes etl_inserted des tables sources
        wide_orders_data = (
            wide_orders_data
            .drop(orders_data["etl_inserted"])
            .drop(customer_data["etl_inserted"])
            .drop(part_data["etl_inserted"])
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_orders_data,
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
        selected_columns = [
            # Clés
            col("order_key"),
            col("line_number"),
            # Informations commande
            col("order_date"),
            col("order_status"),
            col("order_priority"),
            # Métriques financières
            col("extended_price"),
            col("net_amount"),
            col("discount_amount"),
            col("tax_amount"),
            # Informations client
            col("customer_name"),
            col("market_segment"),
            col("nation_name").alias("customer_nation"),
            col("region_name").alias("customer_region"),
            # Informations produit
            col("part_name"),
            col("manufacturer"),
            col("brand"),
            col("product_type"),
            col("manufacturing_country"),
            col("manufacturing_region"),
            # Métriques livraison
            col("shipping_delay_days"),
            col("delivery_delay_days"),
            col("is_late_delivery"),
            # Métadonnées
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

        wide_orders_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
            .select(selected_columns)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=wide_orders_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset