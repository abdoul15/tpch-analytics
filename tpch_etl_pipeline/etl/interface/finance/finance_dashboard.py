from pyspark.sql.functions import col
from pyspark.sql import SparkSession


def create_finance_dashboard_view(
    finance_metrics_data,
    spark: SparkSession,
    storage_path='s3a://spark-bucket/delta/interface/finance/dashboard',
):
    """
    Crée une vue pour le tableau de bord financier destiné au département Finance & Comptabilité.

    Cette vue expose les métriques financières clés comme les revenus, les taxes, les remises,
    les créances et les marges estimées.

    Args:
        finance_metrics_data: DataFrame contenant les données de métriques financières
        spark: Instance SparkSession pour les opérations Spark
        storage_path: Chemin de stockage pour les données Delta
    """
    # Renommer les colonnes pour plus de clarté business (sans espaces ni caractères spéciaux)
    renamed_data = finance_metrics_data.select(
        col('date').alias('date'),
        col('customer_nation').alias('pays_client'),
        col('customer_region').alias('region_client'),
        # Métriques financières
        col('total_revenue').alias('revenu_total'),
        col('total_tax').alias('taxes_totales'),
        col('total_discounts').alias('remises_totales'),
        col('accounts_receivable').alias('creances_clients'),
        col('estimated_margin').alias('marge_estimee'),
        col('margin_percentage').alias('taux_marge_pct'),
        # Métriques de commande
        col('order_count').alias('nombre_commandes'),
        col('average_order_value').alias('valeur_moyenne_commande'),
        col('avg_open_order_age').alias('age_moyen_commandes_jours'),
    )

    # Écrire en tant que table Delta dans MinIO
    renamed_data.write.format('delta').mode('overwrite').save(storage_path)

    return renamed_data
