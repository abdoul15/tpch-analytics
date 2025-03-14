from pyspark.sql import SparkSession

from tpch_etl_pipeline.etl.gold.finance_metrics import FinanceMetricsGoldETL
from tpch_etl_pipeline.etl.gold.supply_chain_metrics import SupplyChainMetricsGoldETL
from tpch_etl_pipeline.etl.gold.sales_metrics import SalesMetricsGoldETL
from tpch_etl_pipeline.etl.gold.daily_sales_metrics import DailySalesMetricsGoldETL

from tpch_etl_pipeline.etl.interface.daily_sales_report import create_daily_sales_report_view
from tpch_etl_pipeline.etl.interface.finance.finance_dashboard import (
    create_finance_dashboard_view,
    create_finance_detailed_report_view
)
from tpch_etl_pipeline.etl.interface.supply_chain.supply_chain_dashboard import (
    create_supply_chain_dashboard_view,
    create_supplier_performance_view,
    create_inventory_analysis_view
)
from tpch_etl_pipeline.etl.interface.sales.sales_dashboard import (
    create_sales_dashboard_view,
    create_product_performance_view,
    create_customer_insights_view
)


def initialize_all_views(spark: SparkSession, run_upstream: bool = True, load_data: bool = True):
    """
    Initialise toutes les vues d'interface pour tous les départements.
    
    Cette fonction crée toutes les vues d'interface à partir des données de la couche gold.
    Elle peut optionnellement exécuter les ETL en amont pour générer les données gold.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
        run_upstream: Si True, exécute les ETL en amont pour générer les données gold
        load_data: Si True, charge les données dans le stockage
    """
    print("Initialisation des vues d'interface pour tous les départements...")
    
    # Initialiser les ETL Gold
    finance_etl = FinanceMetricsGoldETL(
        spark=spark,
        run_upstream=run_upstream,
        load_data=load_data
    )
    
    supply_chain_etl = SupplyChainMetricsGoldETL(
        spark=spark,
        run_upstream=run_upstream,
        load_data=load_data
    )
    
    sales_etl = SalesMetricsGoldETL(
        spark=spark,
        run_upstream=run_upstream,
        load_data=load_data
    )
    
    daily_sales_etl = DailySalesMetricsGoldETL(
        spark=spark,
        run_upstream=run_upstream,
        load_data=load_data
    )
    
    # Exécuter les ETL si nécessaire
    if run_upstream:
        print("Exécution des ETL Gold...")
        finance_etl.run()
        supply_chain_etl.run()
        sales_etl.run()
        daily_sales_etl.run()
    
    # Lire les données Gold
    finance_data = finance_etl.read()
    supply_chain_data = supply_chain_etl.read()
    sales_data = sales_etl.read()
    daily_sales_data = daily_sales_etl.read()
    
    # Créer les vues d'interface
    print("Création des vues d'interface...")
    
    # Vues Finance
    create_finance_dashboard_view(finance_data.curr_data)
    create_finance_detailed_report_view(finance_data.curr_data)
    
    # Vues Supply Chain
    create_supply_chain_dashboard_view(supply_chain_data.curr_data)
    create_supplier_performance_view(supply_chain_data.curr_data)
    create_inventory_analysis_view(supply_chain_data.curr_data)
    
    # Vues Sales
    create_sales_dashboard_view(sales_data.curr_data)
    create_product_performance_view(sales_data.curr_data)
    create_customer_insights_view(sales_data.curr_data)
    
    # Vue Daily Sales Report (existante)
    create_daily_sales_report_view(daily_sales_data.curr_data)
    
    print("Toutes les vues d'interface ont été créées avec succès!")
    
    # Retourner un dictionnaire des vues créées pour référence
    return {
        "finance": ["finance_dashboard", "finance_detailed_report"],
        "supply_chain": ["supply_chain_dashboard", "supplier_performance", "inventory_analysis"],
        "sales": ["sales_dashboard", "product_performance", "customer_insights"],
        "general": ["daily_sales_report"]
    }


def initialize_department_views(
    spark: SparkSession,
    department: str,
    run_upstream: bool = True,
    load_data: bool = True
):
    """
    Initialise les vues d'interface pour un département spécifique.
    
    Cette fonction crée les vues d'interface pour un département spécifique
    à partir des données de la couche gold. Elle peut optionnellement exécuter
    les ETL en amont pour générer les données gold.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
        department: Nom du département ('finance', 'supply_chain', 'sales', ou 'all')
        run_upstream: Si True, exécute les ETL en amont pour générer les données gold
        load_data: Si True, charge les données dans le stockage
    """
    if department.lower() == 'all':
        return initialize_all_views(spark, run_upstream, load_data)
    
    print(f"Initialisation des vues d'interface pour le département {department}...")
    
    if department.lower() == 'finance':
        # Initialiser l'ETL Finance
        finance_etl = FinanceMetricsGoldETL(
            spark=spark,
            run_upstream=run_upstream,
            load_data=load_data
        )
        
        # Exécuter l'ETL si nécessaire
        if run_upstream:
            print("Exécution de l'ETL Finance...")
            finance_etl.run()
        
        # Lire les données Finance
        finance_data = finance_etl.read()
        
        # Créer les vues Finance
        create_finance_dashboard_view(finance_data.curr_data)
        create_finance_detailed_report_view(finance_data.curr_data)
        
        print("Vues Finance créées avec succès!")
        return {
            "finance": ["finance_dashboard", "finance_detailed_report"]
        }
    
    elif department.lower() == 'supply_chain':
        # Initialiser l'ETL Supply Chain
        supply_chain_etl = SupplyChainMetricsGoldETL(
            spark=spark,
            run_upstream=run_upstream,
            load_data=load_data
        )
        
        # Exécuter l'ETL si nécessaire
        if run_upstream:
            print("Exécution de l'ETL Supply Chain...")
            supply_chain_etl.run()
        
        # Lire les données Supply Chain
        supply_chain_data = supply_chain_etl.read()
        
        # Créer les vues Supply Chain
        create_supply_chain_dashboard_view(supply_chain_data.curr_data)
        create_supplier_performance_view(supply_chain_data.curr_data)
        create_inventory_analysis_view(supply_chain_data.curr_data)
        
        print("Vues Supply Chain créées avec succès!")
        return {
            "supply_chain": ["supply_chain_dashboard", "supplier_performance", "inventory_analysis"]
        }
    
    elif department.lower() == 'sales':
        # Initialiser l'ETL Sales
        sales_etl = SalesMetricsGoldETL(
            spark=spark,
            run_upstream=run_upstream,
            load_data=load_data
        )
        
        # Exécuter l'ETL si nécessaire
        if run_upstream:
            print("Exécution de l'ETL Sales...")
            sales_etl.run()
        
        # Lire les données Sales
        sales_data = sales_etl.read()
        
        # Créer les vues Sales
        create_sales_dashboard_view(sales_data.curr_data)
        create_product_performance_view(sales_data.curr_data)
        create_customer_insights_view(sales_data.curr_data)
        
        print("Vues Sales créées avec succès!")
        return {
            "sales": ["sales_dashboard", "product_performance", "customer_insights"]
        }
    
    else:
        raise ValueError(
            f"Département '{department}' non reconnu. "
            "Les valeurs valides sont 'finance', 'supply_chain', 'sales', ou 'all'."
        )


if __name__ == "__main__":
    # Exemple d'utilisation
    spark = SparkSession.builder \
        .appName("TPCH ETL Pipeline - Interface Views") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Initialiser toutes les vues
    initialize_all_views(spark)
    
    # Ou initialiser les vues pour un département spécifique
    # initialize_department_views(spark, 'finance')
