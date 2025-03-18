from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum, avg, round, when, date_format, 
    datediff, current_date, count, expr
)

from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet, TableETL


class FinanceMetricsGoldETL(TableETL):
    """
    ETL pour les métriques financières destinées au département Finance & Comptabilité.
    
    Cette classe calcule des métriques financières comme:
    - Revenus et dépenses basés sur les commandes et les paiements
    - Créances et dettes auprès des clients et fournisseurs
    - Coûts et marges bénéficiaires par produit
    """
    
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            WideOrderDetailsGoldETL
        ],
        name: str = "finance_metrics",
        primary_keys: List[str] = ["date", "customer_nation", "customer_region"],
        storage_path: str = "s3a://spark-bucket/delta/gold/finance_metrics",
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

        # Agrégation des métriques financières quotidiennes
        finance_metrics = (
            wide_orders
            .groupBy(
                date_format("order_date", "yyyy-MM-dd").alias("date"),
                "customer_nation",
                "customer_region"
            )
            .agg(
                # Revenus et dépenses
                round(sum("net_amount"), 2).alias("total_revenue"),
                round(sum("tax_amount"), 2).alias("total_tax"),
                round(sum("discount_amount"), 2).alias("total_discounts"),
                
                # Créances (montants dus par les clients)
                round(
                    sum(when(col("order_status") != "F", col("net_amount")).otherwise(0)),
                    2
                ).alias("accounts_receivable"),
                
                # Nombre de commandes et valeur moyenne
                count("order_key").alias("order_count"),
                round(avg("net_amount"), 2).alias("average_order_value"),
                
                # Analyse des marges
                round(
                    sum("net_amount") - sum("extended_price") * 0.8,  # Hypothèse: coût = 80% du prix de base
                    2
                ).alias("estimated_margin"),
                
                # Taux de marge estimé
                round(
                    (sum("net_amount") - sum("extended_price") * 0.8) / sum("net_amount") * 100,
                    2
                ).alias("margin_percentage"),
                
                # Âge moyen des commandes non finalisées (en jours)
                round(
                    avg(
                        when(
                            col("order_status") != "F",
                            datediff(current_date(), col("order_date"))
                        )
                    ),
                    1
                ).alias("avg_open_order_age")
            )
            .withColumn("etl_inserted", lit(current_timestamp))
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=finance_metrics,
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
            col("customer_nation"),
            col("customer_region"),
            
            # Métriques financières
            col("total_revenue"),
            col("total_tax"),
            col("total_discounts"),
            col("accounts_receivable"),
            col("order_count"),
            col("average_order_value"),
            col("estimated_margin"),
            col("margin_percentage"),
            col("avg_open_order_age"),
            
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
                finance_metrics = (
                    self.spark.read.format(self.data_format)
                    .option("versionAsOf", latest_version)
                    .load(self.storage_path)
                    .select(selected_columns)
                )
                
                # Créer l'ETLDataSet et retourner
                etl_dataset = ETLDataSet(
                    name=self.name,
                    curr_data=finance_metrics,
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

        finance_metrics = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
            .select(selected_columns)
        )

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=finance_metrics,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
