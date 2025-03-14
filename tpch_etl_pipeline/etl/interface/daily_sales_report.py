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
    # Rename columns pour plus de clarté business
    renamed_data = daily_sales_metrics_data.select(
        col("date").alias("Date"),
        col("market_segment").alias("Market Segment"),
        col("region").alias("Region"),
        col("total_sales").alias("Total Revenue"),
        col("average_order_value").alias("Average Order Value"),
        col("number_of_orders").alias("Order Count"),
        col("late_delivery_percentage").alias("Late Delivery %"),
        col("fulfillment_rate").alias("Fulfillment Rate %")
    )

    # 1. Créer une vue temporaire globale pour un accès rapide
    renamed_data.createOrReplaceGlobalTempView("daily_sales_report")
    
    # 2. Créer une table permanente dans le catalogue pour un accès via SQL
    renamed_data.write.format("delta").mode("overwrite").saveAsTable(
        "tpchdb.daily_sales_report_view"
    )
    
    print("Vue 'daily_sales_report' créée avec succès (temporaire et permanente)")
    
    return renamed_data
