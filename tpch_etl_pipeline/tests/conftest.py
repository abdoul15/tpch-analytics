# Configuration des fixtures pour les tests
import pytest
import tempfile
import shutil
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from datetime import datetime


@pytest.fixture(scope='session')
def spark():
    """Fixture pour créer une session Spark pour les tests."""
    spark = (
        SparkSession.builder.master('local[*]')
        .appName('TPCH Test Suite')
        .config('spark.sql.warehouse.dir', '/tmp/spark-warehouse')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config(
            'spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        )
        .getOrCreate()
    )

    # Définir le niveau de log pour réduire le bruit pendant les tests
    spark.sparkContext.setLogLevel('ERROR')

    yield spark

    # Nettoyage après les tests
    spark.stop()


@pytest.fixture
def sample_customer_data(spark):
    """Fixture pour générer des données customer de test."""
    pandas_df = pd.DataFrame(
        {
            'c_custkey': [1, 2, 3, 4, 5],
            'c_name': [
                'Customer 1',
                'Customer 2',
                'Customer 3',
                'Customer 4',
                'Customer 5',
            ],
            'c_address': [
                'Address 1',
                'Address 2',
                'Address 3',
                'Address 4',
                'Address 5',
            ],
            'c_nationkey': [1, 1, 2, 3, 3],
            'c_phone': [
                '123-456-7890',
                '234-567-8901',
                '345-678-9012',
                '456-789-0123',
                '567-890-1234',
            ],
            'c_acctbal': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
            'c_mktsegment': [
                'BUILDING',
                'AUTOMOBILE',
                'MACHINERY',
                'HOUSEHOLD',
                'BUILDING',
            ],
            'c_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_nation_data(spark):
    """Fixture pour générer des données nation de test."""
    pandas_df = pd.DataFrame(
        {
            'n_nationkey': [1, 2, 3, 4],
            'n_name': ['FRANCE', 'GERMANY', 'CANADA', 'JAPAN'],
            'n_regionkey': [1, 1, 2, 3],
            'n_comment': ['Comment 1', 'Comment 2', 'Comment 3', 'Comment 4'],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_region_data(spark):
    """Fixture pour générer des données region de test."""
    pandas_df = pd.DataFrame(
        {
            'r_regionkey': [1, 2, 3],
            'r_name': ['EUROPE', 'AMERICA', 'ASIA'],
            'r_comment': ['Comment 1', 'Comment 2', 'Comment 3'],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_orders_data(spark):
    """Fixture pour générer des données orders de test."""
    pandas_df = pd.DataFrame(
        {
            'o_orderkey': [1, 2, 3, 4, 5],
            'o_custkey': [1, 2, 3, 4, 5],
            'o_orderstatus': ['F', 'O', 'F', 'F', 'P'],
            'o_totalprice': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
            'o_orderdate': [
                datetime(2023, 1, 1),
                datetime(2023, 1, 1),
                datetime(2023, 1, 2),
                datetime(2023, 1, 2),
                datetime(2023, 1, 3),
            ],
            'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-LOW', '5-LOW'],
            'o_clerk': ['Clerk#1', 'Clerk#2', 'Clerk#3', 'Clerk#4', 'Clerk#5'],
            'o_shippriority': [0, 0, 0, 0, 0],
            'o_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_lineitem_data(spark):
    """Fixture pour générer des données lineitem de test."""
    pandas_df = pd.DataFrame(
        {
            'l_orderkey': [1, 1, 2, 3, 3],
            'l_partkey': [101, 102, 103, 104, 105],
            'l_suppkey': [1, 2, 3, 4, 5],
            'l_linenumber': [1, 2, 1, 1, 2],
            'l_quantity': [10.0, 5.0, 20.0, 15.0, 8.0],
            'l_extendedprice': [1000.0, 500.0, 2000.0, 1500.0, 800.0],
            'l_discount': [0.1, 0.05, 0.0, 0.2, 0.1],
            'l_tax': [0.08, 0.08, 0.08, 0.08, 0.08],
            'l_returnflag': ['N', 'N', 'N', 'R', 'R'],
            'l_linestatus': ['O', 'O', 'O', 'F', 'F'],
            'l_shipdate': [
                datetime(2023, 1, 10),
                datetime(2023, 1, 10),
                datetime(2023, 1, 15),
                datetime(2023, 1, 20),
                datetime(2023, 1, 20),
            ],
            'l_commitdate': [
                datetime(2023, 1, 15),
                datetime(2023, 1, 15),
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
                datetime(2023, 1, 25),
            ],
            'l_receiptdate': [
                datetime(2023, 1, 20),
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
                datetime(2023, 1, 30),
                datetime(2023, 1, 30),
            ],
            'l_shipinstruct': [
                'DELIVER IN PERSON',
                'TAKE BACK RETURN',
                'DELIVER IN PERSON',
                'NONE',
                'NONE',
            ],
            'l_shipmode': ['AIR', 'RAIL', 'SHIP', 'MAIL', 'TRUCK'],
            'l_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_part_data(spark):
    """Fixture pour générer des données part de test."""
    pandas_df = pd.DataFrame(
        {
            'p_partkey': [101, 102, 103, 104, 105],
            'p_name': ['Part A', 'Part B', 'Part C', 'Part D', 'Part E'],
            'p_mfgr': [
                'Manufacturer#1',
                'Manufacturer#1',
                'Manufacturer#2',
                'Manufacturer#3',
                'Manufacturer#3',
            ],
            'p_brand': ['Brand#1', 'Brand#2', 'Brand#3', 'Brand#4', 'Brand#5'],
            'p_type': ['Type A', 'Type B', 'Type C', 'Type D', 'Type E'],
            'p_size': [10, 20, 30, 40, 50],
            'p_container': [
                'SMALL BOX',
                'MEDIUM BOX',
                'LARGE BOX',
                'JUMBO BOX',
                'WRAP CASE',
            ],
            'p_retailprice': [100.0, 200.0, 300.0, 400.0, 500.0],
            'p_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_supplier_data(spark):
    """Fixture pour générer des données supplier de test."""
    pandas_df = pd.DataFrame(
        {
            's_suppkey': [1, 2, 3, 4, 5],
            's_name': [
                'Supplier 1',
                'Supplier 2',
                'Supplier 3',
                'Supplier 4',
                'Supplier 5',
            ],
            's_address': [
                'Address 1',
                'Address 2',
                'Address 3',
                'Address 4',
                'Address 5',
            ],
            's_nationkey': [1, 1, 2, 3, 3],
            's_phone': [
                '123-456-7890',
                '234-567-8901',
                '345-678-9012',
                '456-789-0123',
                '567-890-1234',
            ],
            's_acctbal': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
            's_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_partsupp_data(spark):
    """Fixture pour générer des données partsupp de test."""
    pandas_df = pd.DataFrame(
        {
            'ps_partkey': [101, 102, 103, 104, 105],
            'ps_suppkey': [1, 2, 3, 4, 5],
            'ps_availqty': [100, 200, 300, 400, 500],
            'ps_supplycost': [10.0, 20.0, 30.0, 40.0, 50.0],
            'ps_comment': [
                'Comment 1',
                'Comment 2',
                'Comment 3',
                'Comment 4',
                'Comment 5',
            ],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture
def sample_wide_order_details(spark):
    """Fixture pour générer des données wide_order_details de test."""
    pandas_df = pd.DataFrame(
        {
            'order_key': [1, 1, 2, 3, 3],
            'customer_key': [1, 1, 2, 3, 3],
            'customer_name': [
                'Customer 1',
                'Customer 1',
                'Customer 2',
                'Customer 3',
                'Customer 3',
            ],
            'market_segment': [
                'BUILDING',
                'BUILDING',
                'AUTOMOBILE',
                'MACHINERY',
                'MACHINERY',
            ],
            'customer_region': ['EUROPE', 'EUROPE', 'EUROPE', 'AMERICA', 'AMERICA'],
            'order_date': [
                datetime(2023, 1, 1),
                datetime(2023, 1, 1),
                datetime(2023, 1, 1),
                datetime(2023, 1, 2),
                datetime(2023, 1, 2),
            ],
            'order_status': ['F', 'F', 'O', 'F', 'P'],
            'line_number': [1, 2, 1, 1, 2],
            'part_key': [101, 102, 103, 104, 105],
            'part_name': ['Part A', 'Part B', 'Part C', 'Part D', 'Part E'],
            'quantity': [10, 5, 20, 15, 8],
            'extended_price': [1000.0, 500.0, 2000.0, 1500.0, 800.0],
            'discount': [0.1, 0.05, 0.0, 0.2, 0.1],
            'net_amount': [900.0, 475.0, 2000.0, 1200.0, 720.0],
            'discount_amount': [100.0, 25.0, 0.0, 300.0, 80.0],
            'ship_date': [
                datetime(2023, 1, 10),
                datetime(2023, 1, 10),
                datetime(2023, 1, 15),
                datetime(2023, 1, 20),
                datetime(2023, 1, 20),
            ],
            'commit_date': [
                datetime(2023, 1, 15),
                datetime(2023, 1, 15),
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
                datetime(2023, 1, 25),
            ],
            'receipt_date': [
                datetime(2023, 1, 20),
                datetime(2023, 1, 20),
                datetime(2023, 1, 25),
                datetime(2023, 1, 30),
                datetime(2023, 1, 30),
            ],
            'shipping_delay_days': [5, 5, 5, 5, 5],
            'delivery_delay_days': [5, 5, 5, 5, 5],
            'is_late_delivery': [False, False, True, False, True],
        }
    )

    return spark.createDataFrame(pandas_df)


@pytest.fixture(scope='module')
def temp_storage_dir():
    """Fixture pour créer un répertoire temporaire pour les données Delta."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Nettoyer après les tests
    shutil.rmtree(temp_dir)
