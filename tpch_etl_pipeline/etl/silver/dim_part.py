from datetime import datetime
from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, round, broadcast
from pyspark.storagelevel import StorageLevel


from tpch_etl_pipeline.etl.bronze.part import PartBronzeETL
from tpch_etl_pipeline.etl.bronze.supplier import SupplierBronzeETL
from tpch_etl_pipeline.etl.bronze.nation import NationBronzeETL
from tpch_etl_pipeline.etl.bronze.region import RegionBronzeETL
from tpch_etl_pipeline.utils.etl_base import ETLDataSet, TableETL
from tpch_etl_pipeline.etl.bronze.partsupp import PartSuppBronzeETL


class DimPartSilverETL(TableETL):
    def __init__(
        self,
        spark: SparkSession,
        upstream_table_names: Optional[List[Type[TableETL]]] = [
            PartBronzeETL,
            PartSuppBronzeETL,
            SupplierBronzeETL,
            NationBronzeETL,
            RegionBronzeETL,
        ],
        name: str = 'dim_part',
        primary_keys: List[str] = ['part_key'],
        storage_path: str = 's3a://spark-bucket/delta/silver/dim_part',
        data_format: str = 'delta',
        database: str = 'tpchdb',
        partition_keys: List[str] = ['etl_inserted'],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        # Mettre en cache les DataFrames pour éviter les recalculs
        part_data = upstream_datasets[0].curr_data.cache()
        partsupp_data = upstream_datasets[1].curr_data.cache()
        supplier_data = upstream_datasets[2].curr_data.cache()
        nation_data = upstream_datasets[3].curr_data.cache()
        region_data = upstream_datasets[4].curr_data.cache()
        current_timestamp = datetime.now()

        # Enrichir avec les données géographiques des fournisseurs
        geo_data = (
            nation_data.join(
                broadcast(region_data),
                nation_data['n_nationkey'] == region_data['r_regionkey'],
                'left',
            )
            .select(
                col('n_nationkey'),
                col('n_name').alias('manufacturing_country'),
                col('r_name').alias('manufacturing_region'),
            )
            .cache()  # Mettre en cache geo_data car il est utilisé dans la jointure suivante
        )

        # Enrichir les données fournisseur avec la géographie
        supplier_enriched = (
            supplier_data.join(
                broadcast(geo_data),
                supplier_data['s_nationkey'] == geo_data['n_nationkey'],
                'left',
            )
            .select(
                col('s_suppkey'),
                col('s_name').alias('supplier_name'),
                col('s_acctbal').alias('supplier_account_balance'),
                col('manufacturing_country'),
                col('manufacturing_region'),
            )
            .cache()  # Mettre en cache supplier_enriched car il est utilisé dans la jointure suivante
        )

        # Transformation des données produit
        part_transformed = part_data.select(
            # Informations produit
            col('p_partkey').alias('part_key'),
            col('p_name').alias('part_name'),
            col('p_mfgr').alias('manufacturer'),
            col('p_brand').alias('brand'),
            col('p_type').alias('product_type'),
            col('p_size').alias('size'),
            col('p_container').alias('container_type'),
            # Informations prix
            col('p_retailprice').alias('retail_price'),
            round(col('p_retailprice') * 0.8, 2).alias('wholesale_price'),
            # Catégorisation
            col('p_type').alias('category'),
            col('p_container').alias('packaging'),
        )

        # Joindre les produits avec PARTSUPP puis avec les fournisseurs enrichis
        transformed_data = (
            part_transformed.join(
                partsupp_data,
                part_transformed['part_key'] == partsupp_data['ps_partkey'],
                'left',
            )
            .join(
                supplier_enriched,
                partsupp_data['ps_suppkey'] == supplier_enriched['s_suppkey'],
                'left',
            )
            # Ajouter les colonnes de PARTSUPP
            .withColumn('supply_cost', col('ps_supplycost'))
            .withColumn('available_qty', col('ps_availqty'))
            .withColumn('etl_inserted', lit(current_timestamp))
            # Supprimer les colonnes de jointure redondantes
            .drop('ps_partkey', 'ps_suppkey')
        )

        # Libérer la mémoire après utilisation
        part_data.unpersist()
        partsupp_data.unpersist()
        supplier_data.unpersist()
        nation_data.unpersist()
        region_data.unpersist()
        geo_data.unpersist()
        supplier_enriched.unpersist()

        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
        return etl_dataset

    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        """Read the transformed data from the Delta Lake table."""
        # Select the desired columns
        selected_columns = [
            col('part_key'),
            col('part_name'),
            col('manufacturer'),
            col('brand'),
            col('product_type'),
            col('size'),
            col('container_type'),
            col('retail_price'),
            col('wholesale_price'),
            col('category'),
            col('packaging'),
            col('supply_cost'),
            col('available_qty'),
            col('supplier_name'),
            col('supplier_account_balance'),
            col('manufacturing_country'),
            col('manufacturing_region'),
            col('etl_inserted'),
        ]

        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data.select(selected_columns),
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        elif partition_values:
            partition_filter = ' AND '.join(
                [f"{k} = '{v}'" for k, v in partition_values.items()]
            )
        else:
            # Trouver la dernière partition etl_inserted directement
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr('max(etl_inserted) as max_etl_inserted')
                .first()[0]
            )

            partition_filter = f"etl_inserted = '{latest_partition}'"

        # Méthode standard si on a un filtre de partition
        dim_part_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
            .filter(partition_filter)
        )

        dim_part_data = dim_part_data.select(selected_columns)

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=dim_part_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
