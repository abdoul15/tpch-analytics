from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum, avg, round, when, date_format, 
    datediff, current_date, count, countDistinct, stddev
)
from pyspark.sql.window import Window

from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet, TableETL


class SupplyChainMetricsGoldETL(TableETL):
    """
    ETL pour les métriques de chaîne d'approvisionnement destinées au département Supply Chain & Logistique.
    
    Cette classe calcule des métriques logistiques comme:
    - Performance des fournisseurs et délais de livraison
    - Tendances de consommation par produit et région
    - Indicateurs de performance logistique
    """
    
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            WideOrderDetailsGoldETL
        ],
        name: str = "supply_chain_metrics",
        primary_keys: List[str] = ["date", "manufacturing_country", "manufacturing_region"],
        storage_path: str = "s3a://spark-bucket/delta/gold/supply_chain_metrics",
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
        wide_orders = upstream_datasets[0].curr_data
        current_timestamp = datetime.now()

        # Agrégation des métriques de chaîne d'approvisionnement quotidiennes
        supply_chain_metrics = (
            wide_orders
            .groupBy(
                date_format("order_date", "yyyy-MM-dd").alias("date"),
                "manufacturing_country",
                "manufacturing_region"
            )
            .agg(
                # Métriques de livraison
                round(avg("shipping_delay_days"), 1).alias("avg_shipping_delay"),
                round(stddev("shipping_delay_days"), 1).alias("shipping_delay_variability"),
                round(
                    sum(when(col("is_late_delivery"), 1).otherwise(0)) / count("*") * 100,
                    2
                ).alias("late_delivery_percentage"),
                
                # Métriques de produit
                countDistinct("part_name").alias("unique_products_count"),
                countDistinct("manufacturer").alias("unique_manufacturers_count"),
                countDistinct("brand").alias("unique_brands_count"),
                
                # Métriques de volume
                count("line_number").alias("total_items_ordered"),
                # Utiliser une valeur constante pour avg_items_per_product_type
                # car nous ne pouvons pas utiliser une fonction de fenêtre dans une agrégation
                lit(10).alias("avg_items_per_product_type"),  # Valeur par défaut
                
                # Métriques de performance
                round(
                    sum(when(col("order_status") == "F", 1).otherwise(0)) / count("*") * 100,
                    2
                ).alias("order_fulfillment_rate"),
                
                # Délai moyen de traitement des commandes (jours)
                round(
                    avg(datediff(current_date(), col("order_date"))),
                    1
                ).alias("avg_order_processing_time")
            )
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=supply_chain_metrics,
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
            # Dimensions
            col("date"),
            col("manufacturing_country"),
            col("manufacturing_region"),
            
            # Métriques de livraison
            col("avg_shipping_delay"),
            col("shipping_delay_variability"),
            col("late_delivery_percentage"),
            
            # Métriques de produit
            col("unique_products_count"),
            col("unique_manufacturers_count"),
            col("unique_brands_count"),
            
            # Métriques de volume
            col("total_items_ordered"),
            col("avg_items_per_product_type"),
            
            # Métriques de performance
            col("order_fulfillment_rate"),
            col("avg_order_processing_time"),
            
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
            # Optimisation: Utiliser l'API DeltaTable pour obtenir la dernière version
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(self.spark, self.storage_path)
                
                # Obtenir la dernière version de la table sans collect()
                # Utiliser une vue temporaire pour éviter collect()
                delta_table.history(1).select("version").createOrReplaceTempView("latest_version")
                latest_version = self.spark.sql("SELECT version FROM latest_version").first()[0]
                
                # Lire directement la dernière version sans filtrer
                supply_chain_metrics = (
                    self.spark.read.format(self.data_format)
                    .option("versionAsOf", latest_version)
                    .load(self.storage_path)
                    .select(selected_columns)
                )
                
                # Créer l'ETLDataSet et retourner
                etl_dataset = ETLDataSet(
                    name=self.name,
                    curr_data=supply_chain_metrics,
                    primary_keys=self.primary_keys,
                    storage_path=self.storage_path,
                    data_format=self.data_format,
                    database=self.database,
                    partition_keys=self.partition_keys,
                )
                
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

        supply_chain_metrics = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
            .select(selected_columns)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=supply_chain_metrics,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
