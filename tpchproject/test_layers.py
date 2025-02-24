from pyspark.sql import SparkSession


from tpchproject.etl.bronze.customer import CustomerBronzeETL
from tpchproject.etl.bronze.part import PartBronzeETL
from tpchproject.etl.bronze.orders import OrdersBronzeETL
from tpchproject.etl.bronze.partsupp import PartSuppBronzeETL


from tpchproject.etl.silver.dim_customer import DimCustomerSilverETL
from tpchproject.etl.silver.dim_part import DimPartSilverETL
from tpchproject.etl.silver.fct_orders import FctOrdersSilverETL



def test_bronze_layer(spark):
    print('=================================')
    print('Testing Bronze Layer')
    print('=================================')

    print('\nTesting Customer Bronze:')
    customer_bronze = CustomerBronzeETL(spark=spark)
    customer_bronze.run()
    customer_df = customer_bronze.read().curr_data
    customer_df.explain(True)
    # print(f'Nombre de lignes dans Customer Bronze: {customer_df.count()}')

    # print('\nTesting Part Bronze:')
    # part_bronze = PartBronzeETL(spark=spark)
    # part_bronze.run()
    # part_df = part_bronze.read().curr_data
    # part_df.show()
    # print(f'Nombre de lignes dans Part Bronze: {part_df.count()}')

    # print('\nTesting Orders Bronze:')
    # orders_bronze = OrdersBronzeETL(spark=spark)
    # orders_bronze.run()
    # orders_df = orders_bronze.read().curr_data
    # orders_df.show()
    # print(f'Nombre de lignes dans Orders Bronze: {orders_df.count()}')

    # print('\nTesting Partsupp Bronze:')
    # partsupp = PartSuppBronzeETL(spark=spark)
    # partsupp.run()
    # partsupp_df = partsupp.read().curr_data
    # orders_df.show()
    # print(f'Nombre de lignes dans Orders Bronze: {orders_df.count()}')


# def test_silver_layer(spark):
#     print("=================================")
#     print("Testing Silver Layer")
#     print("=================================")

#     print("\nTesting DimCustomer Silver:")
#     dim_customer = DimCustomerSilverETL(spark=spark)
#     dim_customer.run()
#     dim_customer.read().curr_data.show()

#     print("\nTesting DimPart Silver:")
#     dim_part = DimPartSilverETL(spark=spark)
#     dim_part.run()
#     dim_part.read().curr_data.show()

#     print("\nTesting FactOrders Silver:")
#     fct_orders = FctOrdersSilverETL(spark=spark)
#     fct_orders.run()
#     fct_orders.read().curr_data.show()




# def get_optimized_spark_session():
#     # Configuration de la mémoire
#     memory_configs = {
#         "spark.driver.memory": "4g",
#         "spark.executor.memory": "4g",
#         "spark.executor.memoryOverhead": "2g",
#         "spark.memory.offHeap.enabled": "true",
#         "spark.memory.offHeap.size": "2g",
#         "spark.memory.fraction": "0.7",
#         "spark.memory.storageFraction": "0.3",
#     }

#     # Configuration des executors
#     executor_configs = {
#         "spark.executor.cores": "2",
#         "spark.executor.instances": "2",
#         "spark.default.parallelism": "8",
#         "spark.sql.shuffle.partitions": "8",
#     }

#     # Configuration des timeouts et retries
#     reliability_configs = {
#         "spark.task.maxFailures": "4",
#         "spark.network.timeout": "800s",
#         "spark.executor.heartbeatInterval": "60s",
#         "spark.storage.blockManagerSlaveTimeoutMs": "300000",
#         "spark.shuffle.io.maxRetries": "10",
#         "spark.shuffle.io.retryWait": "60s",
#     }

#     # Configuration de l'écriture
#     write_configs = {
#         "spark.sql.files.maxRecordsPerFile": "1000000",
#         "spark.sql.adaptive.enabled": "true",
#         "spark.sql.adaptive.coalescePartitions.enabled": "true",
#         "spark.sql.adaptive.skewJoin.enabled": "true",
#         "spark.sql.adaptive.localShuffleReader.enabled": "true",
#         "spark.sql.shuffle.partitions": "10",
#     }

#     # Compression et sérialisation
#     compression_configs = {
#         "spark.rdd.compress": "true",
#         "spark.shuffle.compress": "true",
#         "spark.shuffle.spill.compress": "true",
#         "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
#     }

#     # Création de la session avec toutes les configurations
#     spark = (SparkSession.builder
#         .appName("TPCH Layer Testing")
#         .enableHiveSupport()
#     )

#     # Ajout de toutes les configurations
#     for configs in [memory_configs, executor_configs, reliability_configs, 
#                    write_configs, compression_configs]:
#         for key, value in configs.items():
#             spark = spark.config(key, value)

#     return spark.getOrCreate()

if __name__ == '__main__':
    # Création de la session avec toutes les configurations
    spark = (SparkSession.builder
        .appName("TPCH Layer Bronze : Customer data")
        .config("spark.sql.shuffle.partitions", "40")
        .config("spark.executor.memory", "4g")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")

    test_bronze_layer(spark)
    #test_silver_layer(spark)
