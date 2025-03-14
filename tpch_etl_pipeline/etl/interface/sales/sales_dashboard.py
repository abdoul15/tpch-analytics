from pyspark.sql.functions import col


def create_sales_dashboard_view(sales_metrics_data):
    """
    Crée une vue pour le tableau de bord commercial destiné au département Commercial & Ventes.
    
    Cette vue expose les métriques commerciales clés comme les revenus, les remises,
    les tendances d'achat des clients et la popularité des produits.
    
    Args:
        sales_metrics_data: DataFrame contenant les données de métriques commerciales
    """
    # Renommer les colonnes pour plus de clarté business
    renamed_data = sales_metrics_data.select(
        col("date").alias("Date"),
        col("market_segment").alias("Segment de Marché"),
        col("customer_region").alias("Région Client"),
        
        # Métriques de vente
        col("total_revenue").alias("Chiffre d'Affaires"),
        col("total_discounts").alias("Remises Totales"),
        col("discount_percentage").alias("Taux de Remise (%)"),
        
        # Métriques de commande
        col("order_count").alias("Nombre de Commandes"),
        col("average_order_value").alias("Panier Moyen"),
        
        # Métriques client
        col("unique_customers").alias("Clients Uniques"),
        col("revenue_per_customer").alias("CA par Client")
    )

    # Créer une vue temporaire globale
    renamed_data.createOrReplaceGlobalTempView("sales_dashboard")
    
    # Créer également une vue permanente dans le catalogue Hive/Spark SQL
    renamed_data.write.format("delta").mode("overwrite").saveAsTable(
        "tpchdb.sales_dashboard_view"
    )
    
    return renamed_data


def create_product_performance_view(sales_metrics_data):
    """
    Crée une vue pour l'analyse de la performance des produits.
    
    Cette vue se concentre sur les métriques de produit pour aider
    à identifier les produits les plus populaires et les tendances d'achat.
    
    Args:
        sales_metrics_data: DataFrame contenant les données de métriques commerciales
    """
    # Sélectionner et renommer les colonnes pertinentes pour l'analyse des produits
    renamed_data = sales_metrics_data.select(
        col("date").alias("Date"),
        col("market_segment").alias("Segment de Marché"),
        col("customer_region").alias("Région Client"),
        
        # Métriques produit
        col("unique_products_sold").alias("Produits Distincts Vendus"),
        col("total_items_sold").alias("Articles Vendus"),
        col("items_per_order").alias("Articles par Commande"),
        col("top_products_count").alias("Nombre Top Produits"),
        
        # Métriques de vente
        col("total_revenue").alias("Chiffre d'Affaires"),
        col("discount_percentage").alias("Taux de Remise (%)"),
        
        # Métadonnées
        col("etl_inserted").alias("Date de Mise à Jour")
    )

    # Créer une vue temporaire globale
    renamed_data.createOrReplaceGlobalTempView("product_performance")
    
    # Créer également une vue permanente dans le catalogue Hive/Spark SQL
    renamed_data.write.format("delta").mode("overwrite").saveAsTable(
        "tpchdb.product_performance_view"
    )
    
    return renamed_data


def create_customer_insights_view(sales_metrics_data):
    """
    Crée une vue pour l'analyse des comportements clients.
    
    Cette vue se concentre sur les métriques client pour aider
    à comprendre les comportements d'achat et identifier les clients les plus rentables.
    
    Args:
        sales_metrics_data: DataFrame contenant les données de métriques commerciales
    """
    # Sélectionner et renommer les colonnes pertinentes pour l'analyse des clients
    renamed_data = sales_metrics_data.select(
        col("date").alias("Date"),
        col("market_segment").alias("Segment de Marché"),
        col("customer_region").alias("Région Client"),
        
        # Métriques client
        col("unique_customers").alias("Nombre de Clients"),
        col("revenue_per_customer").alias("CA par Client"),
        
        # Métriques de commande
        col("order_count").alias("Nombre de Commandes"),
        col("average_order_value").alias("Panier Moyen"),
        col("items_per_order").alias("Articles par Commande"),
        
        # Métriques de priorité
        col("high_priority_order_percentage").alias("Commandes Prioritaires (%)"),
        
        # Métadonnées
        col("etl_inserted").alias("Date de Mise à Jour")
    )

    # Créer une vue temporaire globale
    renamed_data.createOrReplaceGlobalTempView("customer_insights")
    
    # Créer également une vue permanente dans le catalogue Hive/Spark SQL
    renamed_data.write.format("delta").mode("overwrite").saveAsTable(
        "tpchdb.customer_insights_view"
    )
    
    return renamed_data
