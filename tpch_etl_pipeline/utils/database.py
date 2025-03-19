# import os
# from pyspark.sql import SparkSession


# def get_table_from_db(table_name: str, spark: SparkSession,
#                      partition_column: str = None, num_partitions: int = 1):
#     host = os.getenv('UPS_HOST')
#     port = os.getenv('UPS_PORT')
#     db = os.getenv('UPS_DATABASE')
#     user = os.getenv('UPS_USERNAME')
#     password = os.getenv('UPS_PASSWORD')

#     jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'

#     # Configuration de base avec tous les types en string
#     base_properties = {
#         'user': str(user),
#         'password': str(password),
#         'driver': 'org.postgresql.Driver',
#         'fetchsize': '10000'
#     }

#     if partition_column:
#         # 1. Récupérer les min/max pour le partitionnement
#         query = f'(SELECT MIN({partition_column}) as min, MAX({partition_column}) as max FROM {table_name}) as tmp'
#         bounds = spark.read.jdbc(
#             url=jdbc_url,
#             table=query,
#             properties=base_properties
#         ).first()

#         # 2. Configuration du partitionnement avec des valeurs string
#         partition_properties = {
#             **base_properties,
#             'partitionColumn': str(partition_column),
#             'lowerBound': str(bounds['min']),
#             'upperBound': str(bounds['max']),
#             'numPartitions': str(num_partitions)
#         }

#         return spark.read.jdbc(
#             url=jdbc_url,
#             table=table_name,
#             properties=partition_properties
#         )
#     else:
#         return spark.read.jdbc(
#             url=jdbc_url,
#             table=table_name,
#             properties=base_properties
#         )


import os
import time
import random
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from humanfriendly import parse_size 
import logging
from py4j.protocol import Py4JJavaError


def get_table_from_db(
    table_name: str,
    spark: SparkSession,
    partition_column: str = None,
    num_partitions: int = None,  # Devient optionnel
    target_partition_size_mb: int = 128,  # Taille cible par partition (Mo)
    max_retries: int = 5,  # Nombre maximum de tentatives
    initial_backoff: float = 1.0,  # Délai initial en secondes
    max_backoff: float = 30.0,  # Délai maximum en secondes
):
    """
    Lit une table depuis PostgreSQL avec un mécanisme de retry.
    
    Args:
        table_name: Nom de la table à lire
        spark: Session Spark
        partition_column: Colonne de partitionnement (optionnelle)
        num_partitions: Nombre de partitions (optionnel)
        target_partition_size_mb: Taille cible par partition en Mo
        max_retries: Nombre maximum de tentatives
        initial_backoff: Délai initial entre les tentatives en secondes
        max_backoff: Délai maximum entre les tentatives en secondes
        
    Returns:
        DataFrame: Données de la table
    """
    host = os.getenv('UPS_HOST')
    port = os.getenv('UPS_PORT')
    db = os.getenv('UPS_DATABASE')
    user = os.getenv('UPS_USERNAME')
    password = os.getenv('UPS_PASSWORD')
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'

    base_properties = {
        'user': user,
        'password': password,
        'driver': 'org.postgresql.Driver',
        'fetchsize': '10000',
    }

    # Fonction pour effectuer une requête JDBC avec retry
    def execute_with_retry(jdbc_func, *args, **kwargs):
        retries = 0
        backoff = initial_backoff
        
        while retries < max_retries:
            try:
                return jdbc_func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    logging.error(f"Échec après {max_retries} tentatives: {str(e)}")
                    raise
                
                # Calculer le délai avec jitter pour éviter les collisions
                jitter = random.uniform(0.8, 1.2)
                sleep_time = min(backoff * jitter, max_backoff)
                
                logging.warning(
                    f"Erreur de connexion à PostgreSQL (tentative {retries}/{max_retries}): {str(e)}. "
                    f"Nouvelle tentative dans {sleep_time:.2f} secondes..."
                )
                
                time.sleep(sleep_time)
                # Augmenter le délai de manière exponentielle
                backoff = min(backoff * 2, max_backoff)

    # 1. Récupérer la taille totale de la table
    size_query = f"""
        SELECT pg_size_pretty(pg_total_relation_size('{table_name}')) AS size,
               reltuples AS approx_rows
        FROM pg_class
        WHERE relname = '{table_name}'
    """
    
    size_info = execute_with_retry(
        spark.read.jdbc,
        url=jdbc_url, 
        table=f'({size_query}) as tmp', 
        properties=base_properties
    ).first()

    # 2. Calculer le nombre de partitions cible
    if num_partitions is None:
        if size_info and size_info['size']:
            size_bytes = parse_size(size_info['size'])
            num_partitions = max(
                1, int(size_bytes / (target_partition_size_mb * 1024 * 1024))
            )
        else:
            num_partitions = 1  

    # 3. Lecture avec partitionnement JDBC
    if partition_column:
        query = f'(SELECT MIN({partition_column}) as min, MAX({partition_column}) as max FROM {table_name}) as tmp'
        bounds = execute_with_retry(
            spark.read.jdbc,
            url=jdbc_url, 
            table=query, 
            properties=base_properties
        ).first()

        partition_properties = {
            **base_properties,
            'partitionColumn': partition_column,
            'lowerBound': str(bounds['min']),
            'upperBound': str(bounds['max']),
            'numPartitions': str(num_partitions),
        }

        df = execute_with_retry(
            spark.read.jdbc,
            url=jdbc_url, 
            table=table_name, 
            properties=partition_properties
        )
    else:
        df = execute_with_retry(
            spark.read.jdbc,
            url=jdbc_url, 
            table=table_name, 
            properties=base_properties
        )

    return df
