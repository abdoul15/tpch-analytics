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
    create_finance_dashboard_view,
)
from tpch_etl_pipeline.etl.interface.supply_chain.supply_chain_dashboard import (
    create_supply_chain_dashboard_view,
    create_supplier_performance_view,
    create_inventory_analysis_view,
)
from tpch_etl_pipeline.etl.interface.sales.sales_dashboard import (
    create_sales_dashboard_view,
    create_product_performance_view,
    create_customer_insights_view,
)


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

    # supply_chain_metrics = SupplyChainMetricsGoldETL(spark=spark)
    # supply_chain_metrics.run()
    # supply_chain_data = supply_chain_metrics.read().curr_data

    # Création des vues pour chaque département
    print('\nCréation de la vue pour le département Finance:')
    storage_path = 's3a://spark-bucket/delta/interface/finance/dashboard'
    data = create_finance_dashboard_view(finance_data, spark, storage_path)
    print(f'Nombre de lignes = {data.count()}')

    # Note: L'enregistrement de la table dans Trino est maintenant géré par le script register_trino_tables.sh
    # et la commande make register-trino-tables

    # print('\nCréation des vues pour le département Supply Chain:')
    # create_supply_chain_dashboard_view(supply_chain_data)
    # create_supplier_performance_view(supply_chain_data)
    # create_inventory_analysis_view(supply_chain_data)

    print('\nVues créées avec succès!')


def run_pipeline(spark, layers=None):
    """
    Exécute le pipeline complet ou des couches spécifiques.

    Args:
        spark: Instance SparkSession pour les opérations Spark
        layers: Liste des couches à exécuter ('bronze', 'silver', 'gold', 'interface')
               Si None, exécute uniquement la couche interface
    """

    run_interface_layer(spark)


if __name__ == '__main__':
    import sys

    # Création de la session Spark avec les configurations mémoire et support Hive
    spark = (
        SparkSession.builder.appName('TPCH Data Pipeline')
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')

    run_pipeline(spark)
