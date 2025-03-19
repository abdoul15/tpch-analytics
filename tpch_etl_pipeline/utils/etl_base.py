from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Type

import great_expectations as gx
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


class InValidDataException(Exception):
    pass


@dataclass
class ETLDataSet:
    """
    Structure de données pour encapsuler un DataFrame Spark avec ses métadonnées.

    Cette classe sert de conteneur pour un DataFrame Spark et ses métadonnées associées,
    facilitant le passage des données entre les différentes étapes du processus ETL.

    Attributs:
        name: Nom du dataset
        curr_data: DataFrame Spark contenant les données actuelles
        primary_keys: Liste des colonnes formant la clé primaire
        storage_path: Chemin où les données sont/seront stockées
        data_format: Format des données (ex: 'delta')
        database: Nom de la base de données
        partition_keys: Liste des colonnes utilisées pour le partitionnement
    """

    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    data_format: str
    database: str
    partition_keys: List[str]


class TableETL(ABC):
    """
    Classe de base abstraite pour les opérations ETL de table.

    Cette classe définit le flux de travail ETL standard pour les tables dans le pipeline de données.
    Elle fournit des méthodes pour extraire des données des sources en amont, les transformer,
    les valider à l'aide de Great Expectations, et les charger dans le stockage cible.

    Toutes les classes ETL spécifiques aux tables doivent hériter de cette classe et implémenter
    les méthodes abstraites.

    Attributs:
        spark: Instance SparkSession pour les opérations Spark
        upstream_table_names: Liste des classes ETL des tables en amont
        name: Nom de la table
        primary_keys: Liste des noms de colonnes de clé primaire
        storage_path: Chemin où les données de la table seront stockées
        data_format: Format pour le stockage des données (ex: 'delta')
        database: Nom de la base de données
        partition_keys: Liste des colonnes pour partitionner les données
        run_upstream: Indique s'il faut exécuter les processus ETL en amont
        load_data: Indique s'il faut charger les données dans le stockage
    """

    @abstractmethod
    def __init__(
        self,
        spark,
        upstream_table_names: Optional[List[Type[TableETL]]],
        name: str,
        primary_keys: List[str],
        storage_path: str,
        data_format: str,
        database: str,
        partition_keys: List[str],
        run_upstream: bool = True,
        load_data: bool = True,
    ) -> None:
        self.spark = spark
        self.upstream_table_names = upstream_table_names
        self.name = name
        self.primary_keys = primary_keys
        self.storage_path = storage_path
        self.data_format = data_format
        self.database = database
        self.partition_keys = partition_keys
        self.run_upstream = run_upstream
        self.load_data = load_data

    @abstractmethod
    def extract_upstream(self) -> List[ETLDataSet]:
        """
        Extrait les données des sources en amont.

        Cette méthode abstraite doit être implémentée par les sous-classes pour définir
        comment les données sont extraites des systèmes sources ou des tables en amont.

        Returns:
            List[ETLDataSet]: Une liste d'objets ETLDataSet contenant les données extraites
        """
        pass

    @abstractmethod
    def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
        pass

    def validate(self, data: ETLDataSet) -> bool:
        """
        Valide les données en utilisant Great Expectations.

        Cette méthode vérifie si les données transformées respectent les attentes définies
        dans un fichier JSON de Great Expectations. Elle crée un contexte GX, une source de données
        temporaire, et exécute la validation sur le DataFrame.

        Args:
            data: L'objet ETLDataSet contenant les données à valider

        Returns:
            bool: True si la validation réussit, False sinon
        """

        ge_path = 'tpch_etl_pipeline/gx'
        expc_json_path = f'{ge_path}/expectations/{self.name}.json'
        file_path = Path(expc_json_path)

        if file_path.exists():
            try:
                # Créer un contexte Great Expectations
                context = gx.get_context(
                    context_root_dir=os.path.join(
                        os.getcwd(),
                        'tpch_etl_pipeline',
                        'gx',
                    )
                )

                # Créer une source de données Spark temporaire
                datasource = context.sources.add_or_update_spark(
                    name='temp_spark_datasource'
                )

                # Créer un asset pour le DataFrame
                asset = datasource.add_dataframe_asset(name=self.name)

                # Créer une requête de lot
                batch_request = asset.build_batch_request(dataframe=data.curr_data)

                # Utiliser le nom de la suite d'attentes sans le layer
                expectation_suite_name = self.name

                # Exécuter la validation
                checkpoint = context.add_or_update_checkpoint(
                    name='temp_checkpoint',
                    validations=[
                        {
                            'batch_request': batch_request,
                            'expectation_suite_name': expectation_suite_name,
                        }
                    ],
                )

                # Exécuter le checkpoint
                try:
                    result = checkpoint.run()
                    success = result.success

                    if not success:
                        print(
                            f'Validation failed for {self.name}. See validation results for details.'
                        )

                    return success
                except Exception as e:
                    print(f'Error validating {self.name}: {str(e)}')
                    return False

            except Exception as e:
                print(f'Error validating {self.name}: {str(e)}')
                return False
        else:
            return True

    def load(self, data: ETLDataSet) -> None:
        """
        Charge les données transformées dans le stockage cible.

        Cette méthode écrit les données transformées au format Delta dans MinIO S3
        et crée une table dans le catalogue Spark SQL pour faciliter l'accès via SQL.

        Args:
            data: L'objet ETLDataSet contenant les données à charger
        """

        target_partitions = data.curr_data.rdd.getNumPartitions()

        # Utiliser repartition au lieu de coalesce pour une meilleure distribution
        data.curr_data.repartition(target_partitions).write.option(
            'mergeSchema', 'true'
        ).format(data.data_format).mode('overwrite').partitionBy(
            data.partition_keys
        ).save(data.storage_path)

        # Optimisation des fichiers Delta
        delta_table = DeltaTable.forPath(self.spark, self.storage_path)
        delta_table.optimize().executeCompaction()

    def run(self) -> None:
        """
        Exécute le processus ETL complet pour la table.

        Cette méthode orchestre l'exécution du processus ETL complet:
        1. Extrait les données des sources en amont
        2. Transforme les données extraites
        3. Valide les données transformées (si activé)
        4. Charge les données transformées dans le stockage (si activé)

        Returns:
            ETLDataSet: Un objet ETLDataSet contenant les données finales
        """
        transformed_data = self.transform_upstream(self.extract_upstream())
        if not self.validate(transformed_data):
            raise InValidDataException(
                f'The {self.name} dataset did not pass validation, please'
                ' check the validation results for more information'
            )
        if self.load_data:
            self.load(transformed_data)

        # from delta.tables import DeltaTable

        # # Lire la table Delta
        # delta_table = DeltaTable.forPath(self.spark, self.storage_path)

        # # Afficher les détails (dont le nombre de fichiers)
        # details = delta_table.detail().collect()[0]
        # print(f"Nombre de fichiers : {details['numFiles']}")
        # print(f"Taille totale : {details['sizeInBytes'] / (1024 ** 2):.2f} Mo")

    @abstractmethod
    def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
        """
        Lit les données de la table depuis le stockage.

        Cette méthode lit les données de la table depuis l'emplacement de stockage défini.
        Si des valeurs de partition sont fournies, elle filtre les données en conséquence.
        Sinon, elle lit la partition la plus récente basée sur la colonne etl_inserted.

        Args:
            partition_values: Dictionnaire des valeurs de partition pour filtrer les données

        Returns:
            ETLDataSet: Un objet ETLDataSet contenant les données lues
        """
        pass
