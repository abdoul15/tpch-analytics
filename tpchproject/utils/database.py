import os
from pyspark.sql import SparkSession


def get_table_from_db(table_name: str, spark: SparkSession):
    host = os.getenv('UPS_HOST')
    port = os.getenv('UPS_PORT')
    db = os.getenv('UPS_DATABASE')
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'
    connection_properties = {
        'user': os.getenv('UPS_USERNAME'),
        'password': os.getenv('UPS_PASSWORD'),
        'driver': 'org.postgresql.Driver',
    }
    return spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )
