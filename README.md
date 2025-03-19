# TPC-H Analytics Pipeline

## Introduction

Ce projet vise à construire un **pipeline analytique complet**, basé sur les **données TPC-H** stockées dans une base **PostgreSQL**.  
L'objectif est de **traiter, transformer et structurer** ces données afin d'extraire des **KPIs métier** exploitables dans un **dashboard** ou une solution de reporting (*Power BI, Tableau, Apache Sperset etc.*).

J'ai adopté une approche **modulaire et scalable** avec une architecture multi-couches **Bronze → Silver → Gold → Interface**, et j'ai mis en place un ensemble de **règles de Data Quality** afin de garantir la fiabilité des résultats.

---

## Besoins Métier

Avant de construire un pipeline, il faut d'abord identifier les besoins spécifiques du client. J'ai supposé que nous avons identifié cela pour un client avec un département Finance & Comptabilité, ayant des besoins distincts pour garantir que les données exposées répondent à leurs attentes.
On peut avoir plusieurs département avec des besoins différents, la logique reste la même.

### **Département Finance & Comptabilité**

**Besoins métier :**
- Analyse des revenus et dépenses basées sur les commandes et les paiements
- Suivi des créances et dettes auprès des clients et fournisseurs
- Calcul des coûts et marges bénéficiaires par produit

**Métriques clés :**
- **Revenu Total** par région et pays client
- **Taxes Totales** et **Remises Totales**
- **Créances Clients** (montants dus par les clients)
- **Marge Estimée** et **Taux de Marge (%)**
- **Âge Moyen des Commandes Ouvertes** (en jours)

---

## Architecture du Pipeline

![Architecture du Pipeline](architecture.png)

### **1. Ingestion (Bronze Layer)**
- Extraction complète des données depuis **PostgreSQL** à chaque exécution (**Full Load**).
- Stockage brut des données dans **Minio**.
- Format de stockage : **Parquet / Delta Lake** .

### **2. Transformation & Modélisation (Silver Layer)**
- Structuration en tables de **Faits** et **Dimensions** .
- Nettoyage des données : typage, gestion des valeurs nulles, uniformisation des formats.
- Application des premières règles de **Data Quality**.

### **3. Calcul des KPIs & Agrégations (Gold Layer)**
- Construction des tables **prêtes pour l'analyse** (One Big Table).
- Calcul des **métriques clés** pour le département Finance :
  - Revenus, taxes, marges, créances

### **4. Exposition des Données (Interface Layer)**
- Vues adaptées aux besoins spécifiques de chaque département
- Données exposées via des **Tables permanentes Delta** dans le catalogue pour les outils BI
- Noms de colonnes adaptés pour une meilleure compréhension métier

### **5. Visualisation (Apache Superset)**
- Tableaux de bord interactifs pour chaque département
- Visualisations personnalisées pour les métriques clés
- Accès sécurisé aux données via une interface web

---

## **Données Source (TPC-H - PostgreSQL)**

Les tables utilisées dans ce projet proviennent du dataset **TPC-H**, qui simule un environnement de **gestion de commandes et de ventes**.

### **Tables Clés**
| Table       | Description |
|-------------|------------|
| `customer`  | Liste des clients |
| `orders`    | Commandes passées par les clients |
| `lineitem`  | Détails des articles commandés |
| `supplier`  | Informations sur les fournisseurs |
| `part`      | Catalogue des produits |
| `nation`    | Référentiel des pays |
| `region`    | Référentiel des régions |


**Volume estimé** : dépend du *scale factor* utilisé pour générer le dataset TPC-H.

---

## **Technologies & Stack**
| Outil / Techno      | Utilisation |
|---------------------|-------------|
| **PostgreSQL**      | Base de données source |
| **Apache Spark** | Traitement des données (ETL) |
| **Parquet / Delta Lake** | Format de stockage optimisé |
| **Apache Superset** | Visualisation et reporting |
| **Great Expectations** | Vérification de la qualité des données |
| **Docker** | Conteneurisation |
| **Minio**| Stockage data lake |

---

## **Démarrage Rapide**

### **1. Installation**

Cloner le dépôt et démarrer l'environnement Docker :

```bash
# Cloner le dépôt
git clone https://github.com/abdoul15/tpch-analytics.git
cd tpch-analytics

# Démarrer l'environnement Docker
make up
```

### **2. Exécution du Pipeline**

Plusieurs options sont disponibles pour exécuter le pipeline :

```bash
# Exécuter uniquement la couche interface (par défaut), ce qui exécute tout le pipeline
make run-pipeline

# Exécuter uniquement les vues pour le département Finance
make run-finance
```

## Configuration de Superset avec Trino pour accéder aux tables Delta

### Architecture de la solution

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Superset  │────▶│    Trino    │────▶│  Delta Lake │
│  (Dashboards)│     │ (SQL Engine)│     │  (Storage)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Trino sert d'intermédiaire entre Superset et les tables Delta stockées dans MinIO(Superset n'a pas de connecteurs pour lire des tables delta stockées sur Minio, il faut donc un intemédiaire). Cette architecture permet :
- Une séparation des préoccupations (Superset se concentre sur la visualisation)
- De meilleures performances grâce au moteur de requête optimisé de Trino
- Un accès direct aux tables Delta sans nécessiter Hive Metastore

### Enregistrement des tables Delta dans Trino

Pour faciliter l'accès aux tables Delta depuis Trino et Superset, J'ai créé un script qui enregistre automatiquement les tables Delta dans Trino. Ce script crée les schémas et les tables dans Trino qui pointent vers les données Delta stockées dans MinIO (cette étape est necessaire).

Pour enregistrer les tables Delta dans Trino :

```bash
make register-trino-tables
```

Cette commande exécute le script `register_trino_tables.sh` qui :
1. Crée le schéma pour le département finance dans Trino s'il n'existe pas
2. Crée les tables dans Trino qui pointent vers les données Delta


### Configuration de la connexion dans Superset

1. Accédez à l'interface Superset à l'adresse http://localhost:8088 (identifiants : admin/admin123)

2. Allez dans **Sources de données > Bases de données > + Base de données**

3. Sélectionnez **Trino**, anciennement **Presto** comme type de base de données

4. Configurez la connexion avec les paramètres suivants :
   - **DISPLAY NAME**: Delta Tables
   - **SQLALCHEMY URI**: trino://trino@trino:8080/delta

5. Testez la connexion et enregistrez

### Création de datasets et dashboards dans Superset

1. Allez dans **Sources de données > Tables > + Table**

2. Sélectionnez la base de données **Delta Tables**

3. Sélectionnez le schéma qui vous intéresse **finance** ou un autre. Vous devez maintenant voir vos différentes métriques

4. Vous pouvez maintenant créer des visualisations et des dashboards à partir de ces tables

---

## **Vues Disponibles**

### **Finance & Comptabilité**

| Nom de la Vue | Description | Tables/Vues |
|---------------|-------------|-------------|
| **Finance Dashboard** | Tableau de bord financier avec les métriques clés | `finance.finance_dashboard_view` |

---

## **Prochaines Étapes (Optionnel)**

- Ajout de métriques supplémentaires selon les besoins des départements
- Intégration avec d'autres outils de visualisation (Tableau, Power BI)
- Mise en place d'un orchestrateur (Airflow) pour automatiser les exécutions

