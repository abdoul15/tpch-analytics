from pyspark.sql.functions import col


def create_sales_dashboard_view(sales_metrics_data):
    """
    Crée une vue pour le tableau de bord commercial destiné au département Commercial & Ventes.
    
    Cette vue expose les métriques commerciales clés comme les revenus, les remises,
    les tendances d'achat des clients et la popularité des produits.
    
    Args:
        sales_metrics_data: DataFrame contenant les données de métriques commerciales
    """
    # Renommer les colonnes pour plus de clarté business (format snake_case standardisé)
    renamed_data = sales_metrics_data.select(
        col("date").alias("date"),
        col("market_segment").alias("segment_marche"),
        col("customer_region").alias("region_client"),
        
        # Métriques de vente
        col("total_revenue").alias("chiffre_affaires"),
        col("total_discounts").alias("remises_totales"),
        col("discount_percentage").alias("taux_remise_pct"),
        
        # Métriques de commande
        col("order_count").alias("nombre_commandes"),
        col("average_order_value").alias("panier_moyen"),
        
        # Métriques client
        col("unique_customers").alias("clients_uniques"),
        col("revenue_per_customer").alias("ca_par_client")
    )

    # Créer une table permanente dans le catalogue Hive/Spark SQL
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
    # Sélectionner et renommer les colonnes pertinentes pour l'analyse des produits (format snake_case standardisé)
    renamed_data = sales_metrics_data.select(
        col("date").alias("date"),
        col("market_segment").alias("segment_marche"),
        col("customer_region").alias("region_client"),
        
        # Métriques produit
        col("unique_products_sold").alias("produits_distincts_vendus"),
        col("total_items_sold").alias("articles_vendus"),
        col("items_per_order").alias("articles_par_commande"),
        col("top_products_count").alias("nombre_top_produits"),
        
        # Métriques de vente
        col("total_revenue").alias("chiffre_affaires"),
        col("discount_percentage").alias("taux_remise_pct"),
        
        # Métadonnées
        col("etl_inserted").alias("date_mise_a_jour")
    )

    # Créer une table permanente dans le catalogue Hive/Spark SQL
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
    # Sélectionner et renommer les colonnes pertinentes pour l'analyse des clients (format snake_case standardisé)
    renamed_data = sales_metrics_data.select(
        col("date").alias("date"),
        col("market_segment").alias("segment_marche"),
        col("customer_region").alias("region_client"),
        
        # Métriques client
        col("unique_customers").alias("nombre_clients"),
        col("revenue_per_customer").alias("ca_par_client"),
        
        # Métriques de commande
        col("order_count").alias("nombre_commandes"),
        col("average_order_value").alias("panier_moyen"),
        col("items_per_order").alias("articles_par_commande"),
        
        # Métriques de priorité
        col("high_priority_order_percentage").alias("commandes_prioritaires_pct"),
        
        # Métadonnées
        col("etl_inserted").alias("date_mise_a_jour")
    )

    # Créer une table permanente dans le catalogue Hive/Spark SQL
    renamed_data.write.format("delta").mode("overwrite").saveAsTable(
        "tpchdb.customer_insights_view"
    )
    
    return renamed_data
