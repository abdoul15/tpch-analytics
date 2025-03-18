from pyspark.sql.functions import col


def create_daily_sales_report_view(daily_sales_metrics_data):
    """
    Crée une vue pour le rapport de ventes quotidiennes.
    
    Cette fonction renomme les colonnes pour plus de clarté business et crée:
    1. Une vue temporaire globale pour un accès rapide dans la session Spark
    2. Une table permanente dans le catalogue pour un accès via SQL
    
    Args:
        daily_sales_metrics_data: DataFrame contenant les données de métriques de ventes quotidiennes
    """
    # Rename columns pour plus de clarté business (format snake_case standardisé)
    renamed_data = daily_sales_metrics_data.select(
        col("date").alias("date"),
        col("market_segment").alias("segment_marche"),
        col("region").alias("region"),
        col("total_sales").alias("revenu_total"),
        col("average_order_value").alias("valeur_moyenne_commande"),
        col("number_of_orders").alias("nombre_commandes"),
        col("late_delivery_percentage").alias("pourcentage_livraison_tardive"),
        col("fulfillment_rate").alias("taux_execution_pct")
    )

    renamed_data.createOrReplaceGlobalTempView("daily_sales_report")
    
    # # Créer une table permanente dans le catalogue pour un accès via SQL
    # renamed_data.write.format("delta").mode("overwrite").saveAsTable(
    #     "tpchdb.daily_sales_report_view"
    # )

    
    #return renamed_data
    
    #return renamed_data
