from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    sum,
    avg,
    round,
    when,
    date_format,
    count,
    countDistinct,
    desc,
    row_number,
)

from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet, TableETL


class SalesMetricsGoldETL(TableETL):
    """
    ETL pour les métriques commerciales destinées au département Commercial & Ventes.

    Cette classe calcule des métriques commerciales comme:
    - Tendances d'achat des clients
    - Popularité des produits
    - Impact des remises sur les ventes
    """

    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[list[type[TableETL]]] = [
            WideOrderDetailsGoldETL
        ],
        name: str = 'sales_metrics',
        primary_keys: list[str] = ['date', 'market_segment', 'customer_region'],
        storage_path: str = 's3a://spark-bucket/delta/gold/sales_metrics',
        data_format: str = 'delta',
        database: str = 'tpchdb',
        partition_keys: list[str] = ['etl_inserted'],
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

    def extract_upstream(self) -> list[ETLDataSet]:
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

    def transform_upstream(self, upstream_datasets: list[ETLDataSet]) -> ETLDataSet:
        wide_orders = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Préparation des données pour l'analyse des top produits
        window_by_product = Window.partitionBy(
            'date_formatted', 'market_segment', 'customer_region'
        ).orderBy(desc('product_revenue'))

        # Calcul des revenus par produit
        product_revenue = (
            wide_orders.withColumn(
                'date_formatted', date_format('order_date', 'yyyy-MM-dd')
            )
            .groupBy('date_formatted', 'market_segment', 'customer_region', 'part_name')
            .agg(
                round(sum('net_amount'), 2).alias('product_revenue'),
                count('line_number').alias('product_quantity'),
            )
            .withColumn('product_rank', row_number().over(window_by_product))
        )

        # Filtrer pour ne garder que les 5 meilleurs produits par segment/région/jour
        top_products = product_revenue.filter(col('product_rank') <= 5)

        # Agrégation des métriques commerciales quotidiennes
        sales_metrics = (
            wide_orders.groupBy(
                date_format('order_date', 'yyyy-MM-dd').alias('date'),
                'market_segment',
                'customer_region',
            )
            .agg(
                # Métriques de vente
                round(sum('net_amount'), 2).alias('total_revenue'),
                round(sum('discount_amount'), 2).alias('total_discounts'),
                round(sum('discount_amount') / sum('net_amount') * 100, 2).alias(
                    'discount_percentage'
                ),
                # Métriques de commande
                count('order_key').alias('order_count'),
                round(avg('net_amount'), 2).alias('average_order_value'),
                # Métriques client
                countDistinct('customer_name').alias('unique_customers'),
                round(sum('net_amount') / countDistinct('customer_name'), 2).alias(
                    'revenue_per_customer'
                ),
                # Métriques produit
                countDistinct('part_name').alias('unique_products_sold'),
                count('line_number').alias('total_items_sold'),
                round(count('line_number') / count('order_key'), 2).alias(
                    'items_per_order'
                ),
                # Métriques de performance
                round(
                    sum(
                        when(
                            col('order_priority')
                            == '1-URGENT' | col('order_priority')
                            == '2-HIGH',
                            col('net_amount'),
                        ).otherwise(0)
                    )
                    / sum('net_amount')
                    * 100,
                    2,
                ).alias('high_priority_order_percentage'),
            )
            .withColumn('etl_inserted', lit(current_timestamp))
        )

        # Joindre les informations sur les top produits
        # Cette partie serait idéalement implémentée avec une structure de données plus complexe
        # Pour simplifier, nous ajoutons juste un indicateur du nombre de top produits
        sales_metrics_with_top = (
            sales_metrics.join(
                top_products.groupBy(
                    'date_formatted', 'market_segment', 'customer_region'
                ).agg(count('part_name').alias('top_products_count')),
                (sales_metrics['date'] == top_products['date_formatted'])
                & (sales_metrics['market_segment'] == top_products['market_segment'])
                & (sales_metrics['customer_region'] == top_products['customer_region']),
                'left',
            )
            .drop('date_formatted')
            .na.fill({'top_products_count': 0})
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=sales_metrics_with_top,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(self, partition_values: Optional[dict[str, str]] = None) -> ETLDataSet:
        selected_columns = [
            # Dimensions
            col('date'),
            col('market_segment'),
            col('customer_region'),
            # Métriques de vente
            col('total_revenue'),
            col('total_discounts'),
            col('discount_percentage'),
            # Métriques de commande
            col('order_count'),
            col('average_order_value'),
            # Métriques client
            col('unique_customers'),
            col('revenue_per_customer'),
            # Métriques produit
            col('unique_products_sold'),
            col('total_items_sold'),
            col('items_per_order'),
            # Métriques de performance
            col('high_priority_order_percentage'),
            col('top_products_count'),
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

        sales_metrics = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
            .select(selected_columns)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=sales_metrics,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
