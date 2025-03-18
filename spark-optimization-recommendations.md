# Recommandations d'Optimisation pour le Pipeline ETL Spark

## Problèmes Identifiés

Après analyse du code, voici les principaux problèmes de performance identifiés dans le pipeline ETL:

1. **Partitionnement inefficace lors de l'écriture**: L'utilisation de `coalesce(1)` dans la méthode `load()` force toutes les données à être écrites par une seule tâche.
2. **Lecture inefficace des partitions**: La recherche de la partition la plus récente lit toutes les métadonnées de la table.
3. **Exécution séquentielle des tâches en amont**: Les tâches en amont sont exécutées l'une après l'autre.
4. **Jointures non optimisées**: Pas d'utilisation de broadcast joins ou d'optimisation de l'ordre des jointures.
5. **Validation coûteuse**: La validation avec Great Expectations peut ajouter un temps de traitement significatif.
6. **Absence de caching**: Les DataFrames intermédiaires ne sont pas mis en cache.
7. **Partitionnement limité des données**: Les données ne sont partitionnées que par `etl_inserted`.
8. **Configuration Spark sous-optimale**: Certains paramètres Spark pourraient être mieux configurés.

## Solutions Recommandées

### 1. Optimiser le Partitionnement lors de l'Écriture

Remplacer `coalesce(1)` par un nombre approprié de partitions:

```python
# Avant
data.curr_data.coalesce(1).write.option("mergeSchema", "true").format(
    data.data_format
).mode("overwrite").partitionBy(data.partition_keys).save(
    data.storage_path
)

# Après
# Calculer un nombre approprié de partitions basé sur la taille des données
num_partitions = max(1, min(10, int(data.curr_data.count() / 100000)))
data.curr_data.repartition(num_partitions).write.option("mergeSchema", "true").format(
    data.data_format
).mode("overwrite").partitionBy(data.partition_keys).save(
    data.storage_path
)
```

### 2. Optimiser la Lecture des Partitions

Utiliser les métadonnées Delta pour trouver la partition la plus récente:

```python
# Avant
latest_partition = (
    self.spark.read.format(self.data_format)
    .load(self.storage_path)
    .selectExpr("max(etl_inserted)")
    .collect()[0][0]
)

# Après
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(self.spark, self.storage_path)
latest_partition = delta_table.history(1).select("timestamp").collect()[0][0]
```

### 3. Paralléliser l'Exécution des Tâches en Amont

Utiliser des techniques de parallélisation pour les tâches en amont:

```python
# Avant (séquentiel)
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

# Après (parallèle avec multiprocessing)
# Note: Cette approche nécessite une refactorisation plus importante
# et pourrait ne pas être directement applicable sans modifications supplémentaires
```

### 4. Optimiser les Jointures

Utiliser des broadcast joins pour les petites tables:

```python
# Avant
wide_orders_data = (
    orders_data
    .join(
        customer_data,
        orders_data["customer_key"] == customer_data["customer_key"],
        "left"
    )
    .join(
        part_data,
        orders_data["part_key"] == part_data["part_key"],
        "left"
    )
)

# Après
from pyspark.sql.functions import broadcast
wide_orders_data = (
    orders_data
    .join(
        broadcast(customer_data),
        orders_data["customer_key"] == customer_data["customer_key"],
        "left"
    )
    .join(
        broadcast(part_data),
        orders_data["part_key"] == part_data["part_key"],
        "left"
    )
)
```

### 5. Optimiser la Validation

Limiter la validation aux cas critiques ou l'exécuter conditionnellement:

```python
# Ajouter un paramètre pour contrôler la validation
def __init__(
    self,
    # ... autres paramètres ...
    validate_data: bool = True,
):
    # ... initialisation existante ...
    self.validate_data = validate_data

# Dans la méthode run()
def run(self) -> None:
    transformed_data = self.transform_upstream(self.extract_upstream())
    if self.validate_data and not self.validate(transformed_data):
        raise InValidDataException(
            f"The {self.name} dataset did not pass validation"
        )
    if self.load_data:
        self.load(transformed_data)
```

### 6. Utiliser le Caching

Mettre en cache les DataFrames intermédiaires:

```python
# Dans transform_upstream
def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
    orders_data = upstream_datasets[0].curr_data.cache()  # Mettre en cache
    customer_data = upstream_datasets[1].curr_data.cache()  # Mettre en cache
    part_data = upstream_datasets[2].curr_data.cache()  # Mettre en cache
    
    # ... reste de la transformation ...
    
    # Libérer la mémoire après utilisation
    orders_data.unpersist()
    customer_data.unpersist()
    part_data.unpersist()
    
    return etl_dataset
```

### 7. Optimiser le Partitionnement des Données

Partitionner les données par des colonnes fréquemment utilisées:

```python
# Avant
partition_keys: List[str] = ["etl_inserted"]

# Après (exemple pour daily_sales_metrics)
partition_keys: List[str] = ["date", "etl_inserted"]
```

### 8. Configuration Spark Optimale

Ajouter ces configurations à `spark-defaults.conf`:

```
# Optimisations générales
spark.sql.adaptive.enabled                   true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.skewJoin.enabled          true
spark.sql.adaptive.localShuffleReader.enabled true
spark.sql.shuffle.partitions                 200
spark.sql.autoBroadcastJoinThreshold         10485760  # 10 MB

# Optimisations Delta
spark.databricks.delta.optimize.repartition.enabled true
spark.databricks.delta.properties.defaults.enableChangeDataFeed true

# Optimisations de mémoire
spark.memory.fraction                        0.8
spark.memory.storageFraction                 0.3

# Optimisations d'exécution
spark.executor.heartbeatInterval             10s
spark.network.timeout                        120s
```

## Modifications Spécifiques à Implémenter

### 1. Modifier la Méthode `load()` dans `etl_base.py`

```python
def load(self, data: ETLDataSet) -> None:
    print(f"Ecriture des données demarrée pour la table {data.name}")
    
    # Calculer un nombre approprié de partitions
    row_count = data.curr_data.count()
    num_partitions = max(1, min(10, int(row_count / 100000)))
    
    # Utiliser repartition au lieu de coalesce
    data.curr_data.repartition(num_partitions).write.option("mergeSchema", "true").format(
        data.data_format
    ).mode("overwrite").partitionBy(data.partition_keys).save(
        data.storage_path
    )

    print(f"Fin d'ecriture des données pour la table {data.name}")
```

### 2. Optimiser les Jointures dans `wide_order_details.py`

```python
def transform_upstream(self, upstream_datasets: List[ETLDataSet]) -> ETLDataSet:
    orders_data = upstream_datasets[0].curr_data.cache()
    customer_data = upstream_datasets[1].curr_data.cache()
    part_data = upstream_datasets[2].curr_data.cache()
    current_timestamp = datetime.now()

    # Utiliser broadcast pour les petites tables
    from pyspark.sql.functions import broadcast
    
    # Joindre les données de commande avec client et produit
    wide_orders_data = (
        orders_data
        .join(
            broadcast(customer_data),
            orders_data["customer_key"] == customer_data["customer_key"],
            "left"
        )
        .join(
            broadcast(part_data),
            orders_data["part_key"] == part_data["part_key"],
            "left"
        )
        # Supprimer les clés dupliquées
        .drop(customer_data["customer_key"])
        .drop(part_data["part_key"])
    )

    # ... reste du code ...
    
    # Libérer la mémoire
    orders_data.unpersist()
    customer_data.unpersist()
    part_data.unpersist()
    
    return etl_dataset
```

### 3. Optimiser la Lecture des Partitions dans les Méthodes `read()`

```python
def read(self, partition_values: Optional[Dict[str, str]] = None) -> ETLDataSet:
    # ... code existant ...
    
    elif partition_values:
        partition_filter = " AND ".join(
            [f"{k} = '{v}'" for k, v in partition_values.items()]
        )
    else:
        # Utiliser l'historique Delta pour obtenir la dernière partition
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, self.storage_path)
            latest_timestamp = delta_table.history(1).select("timestamp").collect()[0][0]
            
            # Trouver la valeur etl_inserted correspondante
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .where(f"timestamp = '{latest_timestamp}'")
                .select("etl_inserted")
                .limit(1)
                .collect()[0][0]
            )
        except Exception:
            # Fallback à la méthode originale si l'approche Delta échoue
            latest_partition = (
                self.spark.read.format(self.data_format)
                .load(self.storage_path)
                .selectExpr("max(etl_inserted)")
                .collect()[0][0]
            )
        
        partition_filter = f"etl_inserted = '{latest_partition}'"
    
    # ... reste du code ...
```

## Conclusion

Ces optimisations devraient considérablement améliorer les performances du pipeline ETL. Les modifications les plus importantes sont:

1. Remplacer `coalesce(1)` par `repartition(num_partitions)` dans la méthode `load()`
2. Utiliser des broadcast joins pour les tables de dimension
3. Mettre en cache les DataFrames intermédiaires
4. Optimiser la configuration Spark

Il est recommandé d'implémenter ces modifications progressivement et de mesurer l'impact de chaque changement sur les performances.
