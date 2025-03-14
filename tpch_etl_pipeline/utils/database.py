import os
from pyspark.sql import SparkSession




def get_table_from_db(table_name: str, spark: SparkSession, 
                     partition_column: str = None, num_partitions: int = 1):
    host = os.getenv('UPS_HOST')
    port = os.getenv('UPS_PORT')
    db = os.getenv('UPS_DATABASE')
    user = os.getenv('UPS_USERNAME')
    password = os.getenv('UPS_PASSWORD')
    
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'
    
    # Configuration de base avec tous les types en string
    base_properties = {
        'user': str(user),
        'password': str(password),
        'driver': 'org.postgresql.Driver',
        'fetchsize': '10000'
    }
    
    if partition_column:
        # 1. Récupérer les min/max pour le partitionnement
        query = f'(SELECT MIN({partition_column}) as min, MAX({partition_column}) as max FROM {table_name}) as tmp'
        bounds = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=base_properties
        ).first()
        
        # 2. Configuration du partitionnement avec des valeurs string
        partition_properties = {
            **base_properties,
            'partitionColumn': str(partition_column),
            'lowerBound': str(bounds['min']),
            'upperBound': str(bounds['max']),
            'numPartitions': str(num_partitions)
        }
        
        return spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=partition_properties
        )
    else:
        return spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=base_properties
        )