from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, datediff, round, 
    sum, when, current_timestamp,broadcast
)

from tpchproject.etl.bronze.orders import OrdersBronzeETL
from tpchproject.etl.bronze.lineitem import LineItemBronzeETL
from tpchproject.utils.base_table import ETLDataSet,TableETL


class FctOrdersSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            OrdersBronzeETL,
            LineItemBronzeETL
        ],
        name: str = "fct_orders",
        primary_keys: List[str] = ["order_key", "line_number"],
        storage_path: str = "s3a://spark-bucket/delta/silver/fct_orders",
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

    def transform_upstream(
        self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        orders_data = upstream_datasets[0].curr_data
        lineitem_data = upstream_datasets[1].curr_data
        current_timestamp = datetime.now()

        # Enrichir les lignes de commande avec les informations de commande
        transformed_data = (
            lineitem_data
            .join(
                broadcast(orders_data),
                lineitem_data["l_orderkey"] == orders_data["o_orderkey"],
                "inner"
            )
            .select(
                # Clés de dimensions
                col("o_orderkey").alias("order_key"),
                col("l_linenumber").alias("line_number"),
                col("o_custkey").alias("customer_key"),
                col("l_partkey").alias("part_key"),
                col("l_suppkey").alias("supplier_key"),
                
                # Dates
                col("o_orderdate").alias("order_date"),
                col("l_shipdate").alias("ship_date"),
                col("l_commitdate").alias("commit_date"),
                col("l_receiptdate").alias("receipt_date"),
                
                # Métriques de commande
                col("o_totalprice").alias("order_total_amount"),
                col("o_orderpriority").alias("order_priority"),
                col("o_orderstatus").alias("order_status"),
                
                # Métriques de ligne
                col("l_quantity").alias("quantity"),
                col("l_extendedprice").alias("extended_price"),
                col("l_discount").alias("discount_percentage"),
                col("l_tax").alias("tax_percentage"),
                
                # Métriques de livraison
                col("l_shipmode").alias("ship_mode"),
                col("l_returnflag").alias("return_flag"),
                col("l_linestatus").alias("line_status")
            )
            # Calcul des métriques dérivées
            .withColumn(
                "net_amount",
                round(
                    col("extended_price") * (1 - col("discount_percentage")) * 
                    (1 + col("tax_percentage")), 
                    2
                )
            )
            .withColumn(
                "discount_amount",
                round(col("extended_price") * col("discount_percentage"), 2)
            )
            .withColumn(
                "tax_amount",
                round(
                    col("extended_price") * 
                    (1 - col("discount_percentage")) * 
                    col("tax_percentage"), 
                    2
                )
            )
            # Calcul des délais
            .withColumn(
                "shipping_delay_days",
                datediff(col("ship_date"), col("order_date"))
            )
            .withColumn(
                "delivery_delay_days",
                datediff(col("receipt_date"), col("ship_date"))
            )
            # Indicateurs de performance
            .withColumn(
                "is_late_delivery",
                when(
                    col("receipt_date") > col("commit_date"),
                    True
                ).otherwise(False)
            )
            .withColumn("etl_inserted", lit(current_timestamp))
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
            # Clés
            col("order_key"),
            col("line_number"),
            col("customer_key"),
            col("part_key"),
            col("supplier_key"),
            
            # Dates
            col("order_date"),
            col("ship_date"),
            col("commit_date"),
            col("receipt_date"),
            
            # Métriques de commande
            col("order_total_amount"),
            col("order_priority"),
            col("order_status"),
            
            # Métriques de ligne
            col("quantity"),
            col("extended_price"),
            col("discount_percentage"),
            col("tax_percentage"),
            col("net_amount"),
            col("discount_amount"),
            col("tax_amount"),
            
            # Métriques de livraison
            col("ship_mode"),
            col("return_flag"),
            col("line_status"),
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

        fct_orders_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        fct_orders_data = fct_orders_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=fct_orders_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset