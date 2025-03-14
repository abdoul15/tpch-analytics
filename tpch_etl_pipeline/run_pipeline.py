from pyspark.sql import SparkSession

from tpch_etl_pipeline.etl.bronze.customer import CustomerBronzeETL
from tpch_etl_pipeline.etl.bronze.part import PartBronzeETL
from tpch_etl_pipeline.etl.bronze.orders import OrdersBronzeETL
from tpch_etl_pipeline.etl.bronze.partsupp import PartSuppBronzeETL
from tpch_etl_pipeline.etl.bronze.lineitem import LineItemBronzeETL

from tpch_etl_pipeline.etl.silver.dim_customer import DimCustomerSilverETL
from tpch_etl_pipeline.etl.silver.dim_part import DimPartSilverETL
from tpch_etl_pipeline.etl.silver.fct_orders import FctOrdersSilverETL

from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.etl.gold.finance_metrics import FinanceMetricsGoldETL
from tpch_etl_pipeline.etl.gold.supply_chain_metrics import SupplyChainMetricsGoldETL
from tpch_etl_pipeline.etl.interface.finance.finance_dashboard import (
    create_finance_dashboard_view
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


def run_bronze_layer(spark):
    """
    Exécute les ETL de la couche Bronze.
    
    Cette fonction extrait les données brutes depuis PostgreSQL et les stocke
    au format Delta dans MinIO S3.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
    """
    print('=================================')
    print('Exécution de la couche Bronze')
    print('=================================')

    print('\nExécution de Customer Bronze:')
    customer_bronze = CustomerBronzeETL(spark=spark)
    customer_bronze.run()
    
    print('\nExécution de Part Bronze:')
    part_bronze = PartBronzeETL(spark=spark)
    part_bronze.run()

    print('\nExécution de Orders Bronze:')
    orders_bronze = OrdersBronzeETL(spark=spark)
    orders_bronze.run()

    print('\nExécution de Partsupp Bronze:')
    partsupp = PartSuppBronzeETL(spark=spark)
    partsupp.run()

    print('\nExécution de LineItem Bronze:')
    line_item = LineItemBronzeETL(spark=spark)
    line_item.run()


def run_silver_layer(spark):
    """
    Exécute les ETL de la couche Silver.
    
    Cette fonction transforme les données brutes en tables de dimensions et de faits
    pour faciliter l'analyse.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
    """
    print('=================================')
    print('Exécution de la couche Silver')
    print('=================================')

    print('\nExécution de Dim Customer Silver:')
    dim_customer = DimCustomerSilverETL(spark=spark)
    dim_customer.run()
    
    print('\nExécution de Dim Part Silver:')
    dim_part = DimPartSilverETL(spark=spark)
    dim_part.run()

    print('\nExécution de Fact Orders Silver:')
    fct_orders = FctOrdersSilverETL(spark=spark)
    fct_orders.run()


def run_gold_layer(spark):
    """
    Exécute les ETL de la couche Gold.
    
    Cette fonction calcule les métriques et agrégations pour chaque département.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
    """
    print('=================================')
    print('Exécution de la couche Gold')
    print('=================================')

    print('\nExécution de Wide Order Details Gold:')
    wide_orders = WideOrderDetailsGoldETL(spark=spark)
    wide_orders.run()
    
    print('\nExécution de Finance Metrics Gold:')
    finance_metrics = FinanceMetricsGoldETL(spark=spark)
    finance_metrics.run()
    
    print('\nExécution de Supply Chain Metrics Gold:')
    supply_chain_metrics = SupplyChainMetricsGoldETL(spark=spark)
    supply_chain_metrics.run()


def run_interface_layer(spark):
    """
    Exécute les vues de la couche Interface.
    
    Cette fonction crée les vues adaptées à chaque département à partir
    des données de la couche Gold.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
    """
    print('=================================')
    print('Exécution de la couche Interface')
    print('=================================')

    # Lecture des données Gold
    finance_metrics = FinanceMetricsGoldETL(spark=spark)
    finance_metrics.run()
    finance_data = finance_metrics.read().curr_data
    
    supply_chain_metrics = SupplyChainMetricsGoldETL(spark=spark)
    supply_chain_metrics.run()
    supply_chain_data = supply_chain_metrics.read().curr_data

    # Création des vues pour chaque département
    print('\nCréation des vues pour le département Finance:')
    create_finance_dashboard_view(finance_data)
    
    print('\nCréation des vues pour le département Supply Chain:')
    create_supply_chain_dashboard_view(supply_chain_data)
    create_supplier_performance_view(supply_chain_data)
    create_inventory_analysis_view(supply_chain_data)
    
    print('\nVues créées avec succès!')


def run_pipeline(spark, layers=None):
    """
    Exécute le pipeline complet ou des couches spécifiques.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
        layers: Liste des couches à exécuter ('bronze', 'silver', 'gold', 'interface')
               Si None, exécute uniquement la couche interface
    """
    if layers is None:
        layers = ['interface']
    
    if 'bronze' in layers:
        run_bronze_layer(spark)
    
    if 'silver' in layers:
        run_silver_layer(spark)
    
    if 'gold' in layers:
        run_gold_layer(spark)
    
    if 'interface' in layers:
        run_interface_layer(spark)


def run_department_views(spark, department):
    """
    Exécute uniquement les vues pour un département spécifique.
    
    Args:
        spark: Instance SparkSession pour les opérations Spark
        department: Nom du département ('finance', 'supply_chain', 'sales')
    """
    print(f'=================================')
    print(f'Création des vues pour le département {department}')
    print(f'=================================')
    
    if department.lower() == 'finance':
        finance_metrics = FinanceMetricsGoldETL(spark=spark, run_upstream=True)
        finance_metrics.run()
        finance_data = finance_metrics.read().curr_data
        create_finance_dashboard_view(finance_data)
        
        # Afficher un exemple de données
        print('\nExemple de données du tableau de bord financier:')
        spark.sql("SELECT date, pays_client, region_client, revenu_total FROM tpchdb.finance_dashboard_view").show(5)
    
    elif department.lower() == 'supply_chain':
        supply_chain_metrics = SupplyChainMetricsGoldETL(spark=spark, run_upstream=True)
        supply_chain_metrics.run()
        supply_chain_data = supply_chain_metrics.read().curr_data
        create_supply_chain_dashboard_view(supply_chain_data)
        create_supplier_performance_view(supply_chain_data)
        create_inventory_analysis_view(supply_chain_data)
        
        # Afficher un exemple de données
        print('\nExemple de données du tableau de bord supply chain:')
        spark.sql("SELECT date, pays_fabrication, region_fabrication, delai_moyen_expedition_jours FROM tpchdb.supply_chain_dashboard_view").show(5)
    
    else:
        print(f"Département '{department}' non reconnu. Les valeurs valides sont 'finance', 'supply_chain'.")


if __name__ == '__main__':
    import sys
    
    # Création de la session avec toutes les configurations
    spark = (
        SparkSession.builder.appName("TPCH Data Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Vérifier si un argument a été passé
    if len(sys.argv) > 1:
        department = sys.argv[1].lower()
        
        if department == 'finance':
            run_department_views(spark, 'finance')
        elif department == 'supply_chain':
            run_department_views(spark, 'supply_chain')
        elif department == 'all':
            run_pipeline(spark)
        else:
            print(f"Argument '{department}' non reconnu. Les valeurs valides sont 'finance', 'supply_chain', 'all'.")
    else:
        # Par défaut, exécuter le pipeline complet
        run_pipeline(spark)
