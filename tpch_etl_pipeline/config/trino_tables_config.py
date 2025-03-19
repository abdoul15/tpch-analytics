"""
Configuration des tables Delta à enregistrer dans Trino.

Ce module définit les mappings entre les chemins S3 où sont stockées les données Delta
et les noms de tables/schémas dans Trino.
"""

# Configuration des tables Delta et leurs chemins
DELTA_TABLES = {
    # Tables Finance
    'finance': {
        'finance_dashboard_view': 's3a://spark-bucket/delta/interface/finance/dashboard',
    },
    # Tables Supply Chain
    'supply_chain': {
        'supply_chain_dashboard_view': 's3a://spark-bucket/delta/interface/supply_chain/dashboard',
        'supplier_performance_view': 's3a://spark-bucket/delta/interface/supply_chain/supplier_performance',
        'inventory_analysis_view': 's3a://spark-bucket/delta/interface/supply_chain/inventory_analysis',
    },
    # Tables Sales
    'sales': {
        'sales_dashboard_view': 's3a://spark-bucket/delta/interface/sales/dashboard',
        'product_performance_view': 's3a://spark-bucket/delta/interface/sales/product_performance',
        'customer_insights_view': 's3a://spark-bucket/delta/interface/sales/customer_insights',
    },
    # Tables générales
    'general': {
        'daily_sales_report_view': 's3a://spark-bucket/delta/interface/daily_sales_report'
    },
}

# Configuration de Trino
TRINO_CONFIG = {'host': 'trino', 'port': '8080', 'catalog': 'delta', 'user': 'trino'}
