import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import patch

from tpch_etl_pipeline.etl.bronze.customer import CustomerBronzeETL
from tpch_etl_pipeline.etl.bronze.nation import NationBronzeETL
from tpch_etl_pipeline.etl.bronze.region import RegionBronzeETL
from tpch_etl_pipeline.etl.bronze.orders import OrdersBronzeETL
from tpch_etl_pipeline.etl.bronze.lineitem import LineItemBronzeETL
from tpch_etl_pipeline.etl.bronze.part import PartBronzeETL
from tpch_etl_pipeline.etl.silver.dim_customer import DimCustomerSilverETL
from tpch_etl_pipeline.etl.silver.dim_part import DimPartSilverETL
from tpch_etl_pipeline.etl.silver.fct_orders import FctOrdersSilverETL
from tpch_etl_pipeline.etl.gold.wide_order_details import WideOrderDetailsGoldETL
from tpch_etl_pipeline.etl.gold.daily_sales_metrics import DailySalesMetricsGoldETL
from tpch_etl_pipeline.etl.interface.daily_sales_report import (
    create_daily_sales_report_view,
)
from tpch_etl_pipeline.utils.etl_base import ETLDataSet
from tpch_etl_pipeline.etl.bronze.partsupp import PartSuppBronzeETL
from tpch_etl_pipeline.etl.bronze.supplier import SupplierBronzeETL


def test_end_to_end_pipeline(
    spark,
    sample_customer_data,
    sample_nation_data,
    sample_region_data,
    sample_orders_data,
    sample_lineitem_data,
    sample_part_data,
    sample_partsupp_data,
    sample_supplier_data,
    sample_wide_order_details,
    temp_storage_dir,
):
    """Test d'intégration de bout en bout du pipeline complet."""
    # Définir les chemins de stockage temporaires pour toutes les tables
    customer_path = f'{temp_storage_dir}/bronze/customer'
    nation_path = f'{temp_storage_dir}/bronze/nation'
    region_path = f'{temp_storage_dir}/bronze/region'
    orders_path = f'{temp_storage_dir}/bronze/orders'
    lineitem_path = f'{temp_storage_dir}/bronze/lineitem'
    part_path = f'{temp_storage_dir}/bronze/part'
    partsupp_path = f'{temp_storage_dir}/bronze/partsupp'
    supplier_path = f'{temp_storage_dir}/bronze/supplier'

    dim_customer_path = f'{temp_storage_dir}/silver/dim_customer'
    dim_part_path = f'{temp_storage_dir}/silver/dim_part'
    fct_orders_path = f'{temp_storage_dir}/silver/fct_orders'

    wide_order_details_path = f'{temp_storage_dir}/gold/wide_order_details'
    daily_sales_metrics_path = f'{temp_storage_dir}/gold/daily_sales_metrics'

    # Créer et exécuter les ETL de la couche bronze avec des mocks pour l'extraction
    with pytest.MonkeyPatch.context() as mp:
        # Définir des mocks pour toutes les méthodes extract_upstream
        def mock_customer_extract(self):
            return [
                ETLDataSet(
                    name='customer',
                    curr_data=sample_customer_data,
                    primary_keys=['c_custkey'],
                    storage_path=customer_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_nation_extract(self):
            return [
                ETLDataSet(
                    name='nation',
                    curr_data=sample_nation_data,
                    primary_keys=['n_nationkey'],
                    storage_path=nation_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_region_extract(self):
            return [
                ETLDataSet(
                    name='region',
                    curr_data=sample_region_data,
                    primary_keys=['r_regionkey'],
                    storage_path=region_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_orders_extract(self):
            return [
                ETLDataSet(
                    name='orders',
                    curr_data=sample_orders_data,
                    primary_keys=['o_orderkey'],
                    storage_path=orders_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_lineitem_extract(self):
            return [
                ETLDataSet(
                    name='lineitem',
                    curr_data=sample_lineitem_data,
                    primary_keys=['l_orderkey', 'l_linenumber'],
                    storage_path=lineitem_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_part_extract(self):
            return [
                ETLDataSet(
                    name='part',
                    curr_data=sample_part_data,
                    primary_keys=['p_partkey'],
                    storage_path=part_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_part_supp_extract(self):
            return [
                ETLDataSet(
                    name='partsupp',
                    curr_data=sample_partsupp_data,  # Données fictives
                    primary_keys=['ps_partkey', 'ps_suppkey'],
                    storage_path=partsupp_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        def mock_supplier_extract(self):
            return [
                ETLDataSet(
                    name='supplier',
                    curr_data=sample_supplier_data,
                    primary_keys=['s_suppkey'],
                    storage_path=supplier_path,
                    data_format='delta',
                    database='tpchdb',
                    partition_keys=['etl_inserted'],
                )
            ]

        # Appliquer les mocks
        mp.setattr(CustomerBronzeETL, 'extract_upstream', mock_customer_extract)
        mp.setattr(NationBronzeETL, 'extract_upstream', mock_nation_extract)
        mp.setattr(RegionBronzeETL, 'extract_upstream', mock_region_extract)
        mp.setattr(OrdersBronzeETL, 'extract_upstream', mock_orders_extract)
        mp.setattr(LineItemBronzeETL, 'extract_upstream', mock_lineitem_extract)
        mp.setattr(PartBronzeETL, 'extract_upstream', mock_part_extract)
        # Ajouter ces appels de mock
        mp.setattr(PartSuppBronzeETL, 'extract_upstream', mock_part_supp_extract)
        mp.setattr(SupplierBronzeETL, 'extract_upstream', mock_supplier_extract)

        # Exécuter les ETL de la couche bronze
        customer_bronze = CustomerBronzeETL(
            spark=spark, storage_path=customer_path, run_upstream=False
        )
        customer_bronze.run()

        nation_bronze = NationBronzeETL(
            spark=spark, storage_path=nation_path, run_upstream=False
        )
        nation_bronze.run()

        region_bronze = RegionBronzeETL(
            spark=spark, storage_path=region_path, run_upstream=False
        )
        region_bronze.run()

        orders_bronze = OrdersBronzeETL(
            spark=spark, storage_path=orders_path, run_upstream=False
        )
        orders_bronze.run()

        lineitem_bronze = LineItemBronzeETL(
            spark=spark, storage_path=lineitem_path, run_upstream=False
        )
        lineitem_bronze.run()

        part_bronze = PartBronzeETL(
            spark=spark, storage_path=part_path, run_upstream=False
        )
        part_bronze.run()

        part_supp_bronze = PartSuppBronzeETL(
            spark=spark, storage_path=partsupp_path, run_upstream=False
        )
        part_supp_bronze.run()

        supplier_bronze = SupplierBronzeETL(
            spark=spark, storage_path=supplier_path, run_upstream=False
        )
        supplier_bronze.run()

        # Créer des classes wrapper pour les tables en amont
        class CustomerBronzeWrapper(CustomerBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=customer_path, **kwargs)

        class NationBronzeWrapper(NationBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=nation_path, **kwargs)

        class RegionBronzeWrapper(RegionBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=region_path, **kwargs)

        class OrdersBronzeWrapper(OrdersBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=orders_path, **kwargs)

        class LineItemBronzeWrapper(LineItemBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=lineitem_path, **kwargs)

        class PartBronzeWrapper(PartBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=part_path, **kwargs)

        class PartSuppBronzeWrapper(PartSuppBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=partsupp_path, **kwargs)

        class SupplierBronzeWrapper(SupplierBronzeETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=supplier_path, **kwargs)

        # Exécuter les ETL de la couche silver
        dim_customer = DimCustomerSilverETL(
            spark=spark,
            upstream_table_names=[
                CustomerBronzeWrapper,
                NationBronzeWrapper,
                RegionBronzeWrapper,
            ],
            storage_path=dim_customer_path,
            run_upstream=False,
        )
        dim_customer.run()

        dim_part = DimPartSilverETL(
            spark=spark,
            upstream_table_names=[
                PartBronzeWrapper,
                PartSuppBronzeWrapper,
                SupplierBronzeWrapper,
                NationBronzeWrapper,
                RegionBronzeWrapper,
            ],
            storage_path=dim_part_path,
            run_upstream=False,
        )
        dim_part.run()

        fct_orders = FctOrdersSilverETL(
            spark=spark,
            upstream_table_names=[OrdersBronzeWrapper, LineItemBronzeWrapper],
            storage_path=fct_orders_path,
            run_upstream=False,
        )
        fct_orders.run()

        # Créer des classes wrapper pour les tables silver
        class DimCustomerSilverWrapper(DimCustomerSilverETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=dim_customer_path, **kwargs)

        class DimPartSilverWrapper(DimPartSilverETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=dim_part_path, **kwargs)

        class FctOrdersSilverWrapper(FctOrdersSilverETL):
            def __init__(self, spark, **kwargs):
                super().__init__(spark=spark, storage_path=fct_orders_path, **kwargs)

        # Exécuter les ETL de la couche gold
        wide_order_details = WideOrderDetailsGoldETL(
            spark=spark,
            upstream_table_names=[
                FctOrdersSilverWrapper,
                DimCustomerSilverWrapper,
                DimPartSilverWrapper,
            ],
            storage_path=wide_order_details_path,
            run_upstream=False,
        )
        wide_order_details.run()

        # Créer une classe wrapper pour wide_order_details
        class WideOrderDetailsGoldWrapper(WideOrderDetailsGoldETL):
            def __init__(self, spark, **kwargs):
                super().__init__(
                    spark=spark, storage_path=wide_order_details_path, **kwargs
                )

        daily_sales_metrics = DailySalesMetricsGoldETL(
            spark=spark,
            upstream_table_names=[WideOrderDetailsGoldWrapper],
            storage_path=daily_sales_metrics_path,
            run_upstream=False,
        )
        daily_sales_metrics.run()

        # Créer la vue d'interface
        daily_sales_data = daily_sales_metrics.read().curr_data
        create_daily_sales_report_view(daily_sales_data)

        # Vérifier que la vue a été créée
        report_data = spark.sql('SELECT * FROM global_temp.daily_sales_report')

        # Vérifier les colonnes de la vue
        expected_columns = [
            'Date',
            'Market Segment',
            'Region',
            'Total Revenue',
            'Average Order Value',
            'Order Count',
            'Late Delivery %',
            'Fulfillment Rate %',
        ]

        for col_name in expected_columns:
            assert col_name in report_data.columns

        # Vérifier que les données sont cohérentes
        # Vérifier que le nombre de lignes dans la vue correspond au nombre de combinaisons uniques
        # de date, segment de marché et région dans les données de test
        unique_combinations = (
            sample_wide_order_details.select(
                'order_date', 'market_segment', 'customer_region'
            )
            .distinct()
            .count()
        )
        assert report_data.count() == unique_combinations
