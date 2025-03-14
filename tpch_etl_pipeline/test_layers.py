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
from tpch_etl_pipeline.etl.gold.daily_sales_metrics import DailySalesMetricsGoldETL

from tpch_etl_pipeline.etl.interface.daily_sales_report import create_daily_sales_report_view



def test_bronze_layer(spark):
    print('=================================')
    # print('Testing Bronze Layer')
    # print('=================================')

    # print('\nTesting Customer Bronze:')
    # run_upstream=False
    # customer_bronze = CustomerBronzeETL(spark=spark,run_upstream=run_upstream)
    # if run_upstream:  # Vérification explicite
    #     customer_bronze.run()  # Exécution conditionnelle
    # else : 
    #     latest_data = customer_bronze.read().curr_data
    #     print(f"Nombre de ligne = {latest_data.count()}")
    
    # print('\nTesting Part Bronze:')
    # part_bronze = PartBronzeETL(spark=spark)
    # part_bronze.run()
    # part_df = part_bronze.read().curr_data
    # part_df.explain(True)

    # print('\nTesting Orders Bronze:')
    # orders_bronze = OrdersBronzeETL(spark=spark)
    # orders_bronze.run()
    # orders_df = orders_bronze.read().curr_data
    # orders_df.explain()

    # print('\nTesting Partsupp Bronze:')
    # partsupp = PartSuppBronzeETL(spark=spark)
    # partsupp.run()
    # partsupp_df = partsupp.read().curr_data
    # orders_df.explain()


    # line_item = LineItemBronzeETL(spark=spark)
    # #line_item.run()
    # line_item_df = line_item.read().curr_data
    # # print(f"Number of partitions = {line_item_df.rdd.getNumPartitions()}")
    # # print(line_item_df.select("l_shipdate").distinct().count())  # Si > 1000, repensez le partitionnement
    # line_item.getDataFrameStats(line_item,"l_orderkey")
    # # print(f"Column for partition = {line_item.partition_keys}")


def run_code(spark): 
    print("=================================")
    print("Daily Sales Report")
    print("=================================")
    daily_sales = DailySalesMetricsGoldETL(spark=spark)
    daily_sales.run()
    create_daily_sales_report_view(daily_sales.read().curr_data)
    spark.sql("select * from global_temp.daily_sales_report").show()



if __name__ == '__main__':
    # Création de la session avec toutes les configurations
    spark = (
        SparkSession.builder.appName("TPCH Data Pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
        
    
    spark.sparkContext.setLogLevel("ERROR")
    

    #test_bronze_layer(spark)
    #test_silver_layer(spark)
    run_code(spark)
