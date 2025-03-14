from pyspark.sql.functions import col


def create_supply_chain_dashboard_view(supply_chain_metrics_data, storage_path="s3a://spark-bucket/delta/interface/supply_chain/dashboard"):
    """
    Crée une vue pour le tableau de bord logistique destiné au département Supply Chain & Logistique.
    
    Cette vue expose les métriques logistiques clés comme les délais de livraison,
    les taux de livraison tardive, et les indicateurs de performance des fournisseurs.
    
    Args:
        supply_chain_metrics_data: DataFrame contenant les données de métriques logistiques
    """
    # Renommer les colonnes pour plus de clarté business (sans espaces ni caractères spéciaux)
    renamed_data = supply_chain_metrics_data.select(
        col("date").alias("date"),
        col("manufacturing_country").alias("pays_fabrication"),
        col("manufacturing_region").alias("region_fabrication"),
        
        # Métriques de livraison
        col("avg_shipping_delay").alias("delai_moyen_expedition_jours"),
        col("shipping_delay_variability").alias("variabilite_delais_expedition"),
        col("late_delivery_percentage").alias("livraisons_tardives_pct"),
        
        # Métriques de performance
        col("order_fulfillment_rate").alias("taux_execution_commandes_pct"),
        col("avg_order_processing_time").alias("temps_traitement_moyen_jours"),
        
        # Métriques de volume
        col("total_items_ordered").alias("articles_commandes"),
        col("unique_products_count").alias("nombre_produits_uniques")
    )

    # Créer une vue permanente dans le catalogue Hive/Spark SQL et stocker les données dans MinIO S3
    renamed_data.write.format("delta").mode("overwrite") \
        .option("path", storage_path) \
        .saveAsTable("tpchdb.supply_chain_dashboard_view")
    
    return renamed_data


def create_supplier_performance_view(supply_chain_metrics_data, storage_path="s3a://spark-bucket/delta/interface/supply_chain/supplier_performance"):
    """
    Crée une vue pour l'analyse de la performance des fournisseurs.
    
    Cette vue se concentre sur les métriques de performance des fournisseurs
    pour aider à l'évaluation et à l'optimisation de la chaîne d'approvisionnement.
    
    Args:
        supply_chain_metrics_data: DataFrame contenant les données de métriques logistiques
    """
    # Sélectionner et renommer les colonnes pertinentes pour l'analyse des fournisseurs (sans espaces ni caractères spéciaux)
    renamed_data = supply_chain_metrics_data.select(
        col("date").alias("date"),
        col("manufacturing_country").alias("pays_fournisseur"),
        col("manufacturing_region").alias("region_fournisseur"),
        
        # Métriques de performance fournisseur
        col("avg_shipping_delay").alias("delai_moyen_livraison_jours"),
        col("shipping_delay_variability").alias("fiabilite_delais_ecart_type"),
        col("late_delivery_percentage").alias("taux_retard_pct"),
        col("order_fulfillment_rate").alias("taux_execution_pct"),
        
        # Métriques de diversité produit
        col("unique_manufacturers_count").alias("nombre_fabricants"),
        col("unique_brands_count").alias("nombre_marques"),
        col("unique_products_count").alias("nombre_produits"),
        
        # Métadonnées
        col("etl_inserted").alias("date_mise_a_jour")
    )

    # Crée une vue permanente dans le catalogue Hive/Spark SQL et stocker les données dans MinIO S3
    renamed_data.write.format("delta").mode("overwrite") \
        .option("path", storage_path) \
        .saveAsTable("tpchdb.supplier_performance_view")
    
    return renamed_data


def create_inventory_analysis_view(supply_chain_metrics_data, storage_path="s3a://spark-bucket/delta/interface/supply_chain/inventory_analysis"):
    """
    Crée une vue pour l'analyse des stocks et de la consommation.
    
    Cette vue se concentre sur les métriques de volume et de consommation
    pour aider à l'optimisation des niveaux de stock.
    
    Args:
        supply_chain_metrics_data: DataFrame contenant les données de métriques logistiques
    """
    # Sélectionne et renomme les colonnes pertinentes pour l'analyse des stocks (sans espaces ni caractères spéciaux)
    renamed_data = supply_chain_metrics_data.select(
        col("date").alias("date"),
        col("manufacturing_country").alias("pays_origine"),
        col("manufacturing_region").alias("region_origine"),
        
        # Métriques de volume
        col("total_items_ordered").alias("quantite_commandee"),
        col("unique_products_count").alias("nombre_produits_distincts"),
        col("avg_items_per_product_type").alias("quantite_moyenne_par_type"),
        
        # Métriques de traitement
        col("avg_order_processing_time").alias("temps_traitement_jours"),
        
        # Métadonnées
        col("etl_inserted").alias("date_mise_a_jour")
    )

    # Crée une vue permanente dans le catalogue Hive/Spark SQL et stocker les données dans MinIO S3
    renamed_data.write.format("delta").mode("overwrite") \
        .option("path", storage_path) \
        .saveAsTable("tpchdb.inventory_analysis_view")
    
    return renamed_data
