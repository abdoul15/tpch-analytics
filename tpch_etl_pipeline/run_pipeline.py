from pyspark.sql import SparkSession

from tpch_etl_pipeline.etl.gold.finance_metrics import FinanceMetricsGoldETL
from tpch_etl_pipeline.etl.interface.finance.finance_dashboard import (
    create_finance_dashboard_view,
)


def run_interface_layer(spark):
    """
    Exécute les vues de la couche Interface.

    Cette fonction crée les vues adaptées à chaque département (si on en a plusieurs) à partir
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

    print('\nExposition des tables(métriques) pour le département Finance:')
    storage_path = 's3a://spark-bucket/delta/interface/finance/dashboard'
    data = create_finance_dashboard_view(finance_data, spark, storage_path)
    print(f'Nombre de lignes = {data.count()}')


    print('\Tables Exposées avec succès!')


def run_pipeline(spark, layers=None):
    """
    Exécute le pipeline complet.
    Args:
        spark: Instance SparkSession pour les opérations Spark
    """

    run_interface_layer(spark)


if __name__ == '__main__':
    import sys

    # Création de la session Spark avec les configurations mémoire et support Hive
    spark = (
        SparkSession.builder.appName('TPCH Data Pipeline')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')

    run_pipeline(spark)
