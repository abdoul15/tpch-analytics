from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    sum,
    avg,
    count,
    countDistinct,
    round,
    when,
    date_format,
)


from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet, TableETL


class DailySalesMetricsGoldETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            WideOrderDetailsGoldETL
        ],
        name: str = 'daily_sales_metrics',
        primary_keys: List[str] = ['date', 'market_segment', 'region'],
        storage_path: str = 's3a://spark-bucket/delta/gold/daily_sales_metrics',
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
        # Mettre en cache le DataFrame pour éviter les recalculs
        wide_orders = upstream_datasets[0].curr_data.cache()
        current_timestamp = datetime.now()

        # Créer une colonne date formatée pour éviter de la recalculer plusieurs fois
        wide_orders_with_date = wide_orders.withColumn(
            'order_date_formatted', date_format('order_date', 'yyyy-MM-dd')
        )

        # Agrégation des métriques quotidiennes
        daily_metrics = (
            wide_orders_with_date.groupBy(
                'order_date_formatted', 'market_segment', 'customer_region'
            )
            .agg(
                # Métriques de vente
                round(sum('net_amount'), 2).alias('total_sales'),
                round(sum('discount_amount'), 2).alias('total_discounts'),
                round(avg('net_amount'), 2).alias('average_order_value'),
                countDistinct('order_key').alias('number_of_orders'),
                count('line_number').alias('number_of_items'),
                # Métriques de livraison
                round(avg('shipping_delay_days'), 1).alias('avg_shipping_delay'),
                round(avg('delivery_delay_days'), 1).alias('avg_delivery_delay'),
                round(
                    sum(when(col('is_late_delivery'), 1).otherwise(0))
                    / count('*')
                    * 100,
                    2,
                ).alias('late_delivery_percentage'),
                # Métriques par région
                countDistinct('customer_name').alias('unique_customers'),
                countDistinct('part_name').alias('unique_products'),
                # Métriques de performance
                round(
                    sum(
                        when(col('order_status') == 'F', col('net_amount')).otherwise(0)
                    )
                    / sum('net_amount')
                    * 100,
                    2,
                ).alias('fulfillment_rate'),
            )
            .withColumnRenamed('order_date_formatted', 'date')
            .withColumn('etl_inserted', lit(current_timestamp))
        )

        # Libérer la mémoire après utilisation
        wide_orders.unpersist()

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=daily_metrics,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        selected_columns = [
            # Dimensions
            col('date'),
            col('market_segment'),
            col('customer_region').alias('region'),
            # Métriques de vente
            col('total_sales'),
            col('total_discounts'),
            col('average_order_value'),
            col('number_of_orders'),
            col('number_of_items'),
            # Métriques de livraison
            col('avg_shipping_delay'),
            col('avg_delivery_delay'),
            col('late_delivery_percentage'),
            # Métriques business
            col('unique_customers'),
            col('unique_products'),
            col('fulfillment_rate'),
            # Métadonnées
            col('etl_inserted'),
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
            partition_filter = ' AND '.join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            # Trouver la dernière partition etl_inserted directement
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr('max(etl_inserted) as max_etl_inserted')
                .first()[0]
            )

            partition_filter = f"etl_inserted = '{latest_partition}'"

        # Méthode standard si on a un filtre de partition
        daily_metrics = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
            .select(selected_columns)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=daily_metrics,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
