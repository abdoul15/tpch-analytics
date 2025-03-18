# Documentation de la Couche Interface

## Objectif
Cette couche expose les données préparées aux utilisateurs finaux (analystes BI, stakeholders) sous forme de tables Delta optimisées pour l'analyse. Elle constitue la dernière couche de l'architecture ETL (bronze → silver → gold → interface).

## Conventions de Nommage
Toutes les tables d'interface suivent ces conventions de nommage :
- Noms de colonnes en format `snake_case` (avec underscores)
- Noms en français pour une meilleure compréhension par les équipes métier
- Pas d'espaces ni de caractères spéciaux dans les noms de colonnes
- Tables Delta permanentes nommées avec le suffixe `_view` dans le catalogue `tpchdb` (bien que ce ne soient pas des vues SQL mais des tables matérialisées)

## Tables Disponibles par Domaine

### Finance
- **finance_dashboard_view** : Table Delta pour le département Finance & Comptabilité
  - Cas d'utilisation : Suivi des revenus, taxes, remises et marges
  - Colonnes clés : `revenu_total`, `taxes_totales`, `marge_estimee`, `taux_marge_pct`
  - Fréquence de mise à jour : Quotidienne

### Ventes (Sales)
- **sales_dashboard_view** : Table Delta pour le département Commercial & Ventes
  - Cas d'utilisation : Suivi des performances commerciales globales
  - Colonnes clés : `chiffre_affaires`, `remises_totales`, `nombre_commandes`
  - Fréquence de mise à jour : Quotidienne

- **product_performance_view** : Table Delta pour l'analyse des performances produit
  - Cas d'utilisation : Identifier les produits les plus populaires et les tendances d'achat
  - Colonnes clés : `produits_distincts_vendus`, `articles_vendus`, `articles_par_commande`
  - Fréquence de mise à jour : Quotidienne

- **customer_insights_view** : Table Delta pour l'analyse des comportements clients
  - Cas d'utilisation : Comprendre les comportements d'achat et identifier les clients les plus rentables
  - Colonnes clés : `nombre_clients`, `ca_par_client`, `commandes_prioritaires_pct`
  - Fréquence de mise à jour : Quotidienne

### Supply Chain
- **supply_chain_dashboard_view** : Table Delta pour le département Supply Chain & Logistique
  - Cas d'utilisation : Suivi des métriques logistiques clés
  - Colonnes clés : `delai_moyen_expedition_jours`, `livraisons_tardives_pct`, `taux_execution_commandes_pct`
  - Fréquence de mise à jour : Quotidienne

- **supplier_performance_view** : Table Delta pour l'analyse de la performance des fournisseurs
  - Cas d'utilisation : Évaluation et optimisation de la chaîne d'approvisionnement
  - Colonnes clés : `delai_moyen_livraison_jours`, `taux_retard_pct`, `taux_execution_pct`
  - Fréquence de mise à jour : Quotidienne

- **inventory_analysis_view** : Table Delta pour l'analyse des stocks et de la consommation
  - Cas d'utilisation : Optimisation des niveaux de stock
  - Colonnes clés : `quantite_commandee`, `nombre_produits_distincts`, `temps_traitement_jours`
  - Fréquence de mise à jour : Quotidienne

### Général
- **daily_sales_report_view** : Table Delta pour le rapport de ventes quotidiennes
  - Cas d'utilisation : Suivi quotidien des performances de vente
  - Colonnes clés : `revenu_total`, `valeur_moyenne_commande`, `pourcentage_livraison_tardive`
  - Fréquence de mise à jour : Quotidienne

## Utilisation des Tables

### Dans Spark SQL
```sql
-- Exemple d'utilisation d'une table Delta
SELECT 
  date, 
  segment_marche, 
  revenu_total 
FROM tpchdb.daily_sales_report_view
WHERE date >= '2023-01-01'
ORDER BY revenu_total DESC;
```

### Dans des Outils BI
Les tables Delta (`tpchdb.xxx_view`) sont accessibles directement depuis les outils BI compatibles avec Spark SQL, comme Tableau, Power BI, ou Looker.

## Initialisation des Tables
Pour initialiser toutes les tables ou celles spécifiques à un département, utilisez les fonctions dans `initialize_views.py` :

```python
from tpch_etl_pipeline.etl.interface.initialize_views import initialize_all_views, initialize_department_views
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TPCH ETL").getOrCreate()

# Initialiser toutes les tables
initialize_all_views(spark)

# Ou initialiser les tables pour un département spécifique
initialize_department_views(spark, 'finance')
```

## Note Technique
Dans cette couche interface, nous utilisons des tables Delta permanentes créées avec `write.format("delta").mode("overwrite").saveAsTable()`. Ces tables sont stockées au format Delta et sont persistantes. Elles sont nommées avec le suffixe `_view` bien qu'il s'agisse techniquement de tables matérialisées et non de vues SQL.

## Configuration de Superset avec Trino pour accéder aux tables Delta

### Architecture de la solution

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Superset  │────▶│    Trino    │────▶│  Delta Lake │
│  (Dashboards)│     │ (SQL Engine)│     │  (Storage)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Trino sert d'intermédiaire entre Superset et les tables Delta stockées dans MinIO. Cette architecture permet :
- Une séparation des préoccupations (Superset se concentre sur la visualisation)
- De meilleures performances grâce au moteur de requête optimisé de Trino
- Un accès direct aux tables Delta sans nécessiter Hive Metastore

### Prérequis

1. Démarrer les services Trino et Superset :
```bash
make run-bi
```

2. Vérifier que Trino est opérationnel :
```bash
curl http://localhost:8080/v1/info
```

### Accès aux tables Delta via Trino

Trino est configuré pour accéder aux tables Delta stockées dans MinIO en utilisant un catalogue de fichiers (file-based catalog). Cette approche permet d'accéder directement aux tables Delta en spécifiant leur chemin S3.

Pour vérifier que Trino peut accéder aux tables Delta :

1. Connectez-vous à Trino en ligne de commande :
```bash
docker exec -it trino trino
```

2. Interrogez directement une table Delta en spécifiant son chemin S3 :
```sql
SELECT * 
FROM delta."s3a://spark-bucket/delta/interface/finance/dashboard"
LIMIT 5;
```

3. Vous pouvez également explorer les métadonnées de la table :
```sql
DESCRIBE delta."s3a://spark-bucket/delta/interface/finance/dashboard";
```

### Configuration de la connexion dans Superset

1. Accédez à l'interface Superset à l'adresse http://localhost:8088 (identifiants : admin/admin123)

2. Allez dans **Sources de données > Bases de données > + Base de données**

3. Sélectionnez **Trino** comme type de base de données

4. Configurez la connexion avec les paramètres suivants :
   - **DISPLAY NAME**: Delta Tables
   - **SQLALCHEMY URI**: trino://trino@trino:8080/delta

5. Testez la connexion et enregistrez

### Création de datasets et dashboards dans Superset

1. Allez dans **Sources de données > Tables > + Table**

2. Sélectionnez la base de données **Delta Tables**

3. Sélectionnez le schéma **default**

4. Dans le champ **Table**, entrez le chemin complet de la table Delta avec le préfixe du catalogue, par exemple :
   ```
   delta."s3a://spark-bucket/delta/interface/finance/dashboard"
   ```

5. Cliquez sur **Ajouter**

6. Vous pouvez maintenant créer des visualisations et des dashboards à partir de ces tables

> **Note**: Bien que cette approche nécessite de spécifier les chemins S3 complets, elle offre plus de flexibilité et évite d'avoir à enregistrer explicitement chaque table.

### Référence des chemins pour les tables Delta

Pour faciliter l'accès aux tables Delta, voici les chemins complets pour chaque table dans Trino :

| Nom de la table | Chemin dans Trino |
|-----------------|-------------------|
| Finance Dashboard | `delta."s3a://spark-bucket/delta/interface/finance/dashboard"` |
| Supply Chain Dashboard | `delta."s3a://spark-bucket/delta/interface/supply_chain/dashboard"` |
| Supplier Performance | `delta."s3a://spark-bucket/delta/interface/supply_chain/supplier_performance"` |
| Inventory Analysis | `delta."s3a://spark-bucket/delta/interface/supply_chain/inventory_analysis"` |
| Sales Dashboard | `delta."s3a://spark-bucket/delta/interface/sales/dashboard"` |
| Product Performance | `delta."s3a://spark-bucket/delta/interface/sales/product_performance"` |
| Customer Insights | `delta."s3a://spark-bucket/delta/interface/sales/customer_insights"` |
| Daily Sales Report | `delta."s3a://spark-bucket/delta/interface/daily_sales_report"` |

### Dépannage

Si vous rencontrez des problèmes de connexion :

1. Vérifiez que les services Trino et MinIO sont en cours d'exécution :
```bash
docker ps | grep -E 'trino|minio'
```

2. Vérifiez les logs de Trino pour identifier les erreurs spécifiques :
```bash
docker logs trino
```

3. Testez la connexion à Trino directement :
```bash
docker exec -it trino trino --execute "SELECT * FROM delta.default.finance_dashboard_view LIMIT 5"
```

4. Vérifiez que les chemins S3 sont corrects et que les tables Delta existent dans MinIO :
```bash
docker exec -it minio mc ls myminio/spark-bucket/delta/interface/
