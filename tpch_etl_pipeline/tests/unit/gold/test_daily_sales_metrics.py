import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
import pandas as pd
from tpch_etl_pipeline.etl.gold.daily_sales_metrics import DailySalesMetricsGoldETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet


def test_daily_sales_metrics_transform(spark, sample_wide_order_details):
    """Test la transformation de la couche gold pour daily_sales_metrics."""
    # Créer une instance de DailySalesMetricsGoldETL avec un mock pour extract_upstream
    with patch.object(DailySalesMetricsGoldETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        wide_order_details_dataset = ETLDataSet(
            name='wide_order_details',
            curr_data=sample_wide_order_details,
            primary_keys=['order_key', 'line_number'],
            storage_path='s3a://spark-bucket/delta/gold/wide_order_details',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted'],
        )

        mock_extract.return_value = [wide_order_details_dataset]

        # Créer l'instance ETL avec load_data=False
        daily_sales_metrics_etl = DailySalesMetricsGoldETL(spark=spark, load_data=False)

        # Exécuter la transformation
        result = daily_sales_metrics_etl.transform_upstream(
            daily_sales_metrics_etl.extract_upstream()
        )

        # Vérifier que le résultat est correct
        assert result.name == 'daily_sales_metrics'
        assert set(result.primary_keys) == set(['date', 'market_segment', 'region'])

        # Vérifier les colonnes attendues
        expected_columns = [
            'date',
            'market_segment',
            'customer_region',
            'total_sales',
            'total_discounts',
            'average_order_value',
            'number_of_orders',
            'number_of_items',
            'avg_shipping_delay',
            'avg_delivery_delay',
            'late_delivery_percentage',
            'unique_customers',
            'unique_products',
            'fulfillment_rate',
            'etl_inserted',
        ]

        for col_name in expected_columns:
            assert col_name in result.curr_data.columns

        # Vérifier les agrégations
        # Il devrait y avoir 2 dates distinctes dans les données de test
        assert result.curr_data.select('date').distinct().count() == 2

        # Vérifier les métriques pour une date spécifique
        europe_building_day1 = result.curr_data.filter(
            "date = '2023-01-01' AND market_segment = 'BUILDING' AND customer_region = 'EUROPE'"
        ).collect()

        assert len(europe_building_day1) == 1
        assert europe_building_day1[0]['total_sales'] == 1375.0  # 900 + 475
        assert europe_building_day1[0]['number_of_orders'] == 1  # Ordre 1
        assert europe_building_day1[0]['number_of_items'] == 2  # 2 lignes
        assert europe_building_day1[0]['unique_customers'] == 1  # Customer 1
        assert europe_building_day1[0]['unique_products'] == 2  # Part A et Part B


def test_daily_sales_metrics_read(spark, sample_wide_order_details):
    """Test la lecture des données de la couche gold pour daily_sales_metrics."""
    # Créer une instance de DailySalesMetricsGoldETL avec un mock pour extract_upstream
    with patch.object(DailySalesMetricsGoldETL, 'extract_upstream') as mock_extract:
        # Configurer le mock pour retourner nos données de test
        wide_order_details_dataset = ETLDataSet(
            name='wide_order_details',
            curr_data=sample_wide_order_details,
            primary_keys=['order_key', 'line_number'],
            storage_path='s3a://spark-bucket/delta/gold/wide_order_details',
            data_format='delta',
            database='tpchdb',
            partition_keys=['etl_inserted'],
        )

        mock_extract.return_value = [wide_order_details_dataset]

        # Créer l'instance ETL avec load_data=False
        daily_sales_metrics_etl = DailySalesMetricsGoldETL(spark=spark, load_data=False)

        # Exécuter la transformation pour initialiser les données
        daily_sales_metrics_etl.transform_upstream(
            daily_sales_metrics_etl.extract_upstream()
        )

        # Tester la méthode read
        result = daily_sales_metrics_etl.read()

        # Vérifier que le résultat est correct
        assert result.name == 'daily_sales_metrics'
        assert set(result.primary_keys) == set(['date', 'market_segment', 'region'])

        # Vérifier les colonnes attendues
        expected_columns = [
            'date',
            'market_segment',
            'region',
            'total_sales',
            'total_discounts',
            'average_order_value',
            'number_of_orders',
            'number_of_items',
            'avg_shipping_delay',
            'avg_delivery_delay',
            'late_delivery_percentage',
            'unique_customers',
            'unique_products',
            'fulfillment_rate',
            'etl_inserted',
        ]

        for col_name in expected_columns:
            assert col_name in result.curr_data.columns
