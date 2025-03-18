# Optimisations pour améliorer les performances de Spark

Après analyse de votre code et de la configuration Spark, j'ai identifié plusieurs problèmes qui pourraient expliquer la lenteur du pipeline malgré l'utilisation d'un petit ensemble de données TPCH (scale factor = 0.01).

## Problèmes identifiés

### 1. Configuration Spark sous-optimale

```
# Configuration actuelle problématique
spark.sql.codegen.wholeStage false  # Désactive une optimisation majeure
```

L'interface Spark montre :
- 40 cœurs disponibles mais 0 utilisés
- 13.2 GiB de mémoire disponible mais 0.0 B utilisée

Cela suggère que les ressources ne sont pas correctement allouées ou utilisées.

### 2. Exécutions redondantes dans le pipeline ETL

- Dans `run_interface_layer`, les métriques sont recalculées au lieu de simplement lire les données existantes
- Chaque couche ETL exécute les couches en amont, créant des calculs redondants

### 3. Problèmes de parallélisme et de distribution

- Pas de configuration explicite du nombre d'exécuteurs, de cœurs par exécuteur ou de mémoire
- Pas d'optimisation pour la distribution des données et des tâches

### 4. Surcharge de validation et d'E/S

- Validation avec Great Expectations à chaque étape
- Écriture/lecture fréquente depuis MinIO (S3)

## Solutions recommandées

### 1. Optimiser la configuration Spark

Modifiez le fichier `spark/entrypoint.sh` pour ajouter ces configurations :

```bash
# Activer la génération de code en une seule étape (crucial pour les performances)
spark.sql.codegen.wholeStage true

# Optimiser la parallélisation
spark.default.parallelism 40  # Nombre total de cœurs disponibles
spark.sql.shuffle.partitions 40  # Ajuster en fonction du volume de données
spark.executor.cores 10  # 10 cœurs par exécuteur (pour 4 exécuteurs)

# Optimiser la mémoire
spark.executor.memory 3g  # ~3GB par exécuteur
spark.driver.memory 4g
spark.memory.offHeap.enabled true
spark.memory.offHeap.size 2g

# Optimisations Delta Lake
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true

# Optimisations de lecture S3
spark.hadoop.fs.s3a.connection.maximum 100
spark.hadoop.fs.s3a.threads.max 20
```

### 2. Réduire les exécutions redondantes

Modifiez `run_interface_layer` dans `run_pipeline.py` :

```python
def run_interface_layer(spark):
    """
    Exécute les vues de la couche Interface.
    """
    print('=================================')
    print('Exécution de la couche Interface')
    print('=================================')

    # Lecture des données Gold sans recalculer
    finance_metrics = FinanceMetricsGoldETL(spark=spark, run_upstream=False)
    finance_data = finance_metrics.read().curr_data
    
    supply_chain_metrics = SupplyChainMetricsGoldETL(spark=spark, run_upstream=False)
    supply_chain_data = supply_chain_metrics.read().curr_data

    # Création des vues pour chaque département
    print('\nCréation des vues pour le département Finance:')
    create_finance_dashboard_view(finance_data)
    
    print('\nCréation des vues pour le département Supply Chain:')
    create_supply_chain_dashboard_view(supply_chain_data)
    create_supplier_performance_view(supply_chain_data)
    create_inventory_analysis_view(supply_chain_data)
    
    print('\nVues créées avec succès!')
```

### 3. Optimiser le partitionnement des données

Modifiez les classes ETL pour utiliser un partitionnement plus efficace :

```python
# Exemple pour SupplyChainMetricsGoldETL
partition_keys: List[str] = ["date"]  # Partitionner par date au lieu de etl_inserted
```

### 4. Optimiser les transformations Spark

Dans les classes ETL comme `SupplyChainMetricsGoldETL`, optimisez les transformations :

```python
# Ajouter la persistance des DataFrames intermédiaires
wide_orders = upstream_datasets[0].curr_data.cache()  # Mettre en cache pour réutilisation

# Optimiser les agrégations en réduisant les opérations coûteuses
# Par exemple, remplacer les fonctions window par des agrégations plus simples
```

### 5. Désactiver temporairement la validation pour les tests

Pour les tests de performance, vous pouvez désactiver temporairement la validation :

```python
# Dans TableETL.run()
# Remplacer:
if not self.validate(transformed_data):
    raise InValidDataException(...)

# Par:
if self.enable_validation and not self.validate(transformed_data):
    raise InValidDataException(...)
```

### 6. Utiliser la compression pour les données Delta

```python
# Dans la méthode load() de TableETL
data.curr_data.write \
    .option("mergeSchema", "true") \
    .format(data.data_format) \
    .option("compression", "snappy")  # Ajouter la compression
    .mode("overwrite") \
    .partitionBy(data.partition_keys) \
    .option("path", data.storage_path) \
    .saveAsTable(table_name)
```

## Mesures à court terme

Pour un test rapide, vous pouvez modifier `spark/entrypoint.sh` avec les configurations Spark optimisées et exécuter :

```bash
make rebuild
make run-pipeline
```

Cela devrait montrer une amélioration significative des performances.
