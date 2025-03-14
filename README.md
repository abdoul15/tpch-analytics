# TPC-H Analytics Pipeline

## Introduction

Ce projet vise à construire un **pipeline analytique complet**, basé sur les **données TPC-H** stockées dans une base **PostgreSQL**.  
L'objectif est de **traiter, transformer et structurer** ces données afin d'extraire des **KPIs métier** exploitables dans un **dashboard** ou une solution de reporting (*Power BI, Tableau, etc.*).

J'ai adopté une approche **modulaire et scalable** avec une architecture multi-couches **Bronze → Silver → Gold → Interface**, et j'ai mis en place un ensemble de **règles de Data Quality** afin de garantir la fiabilité des résultats.

---

## Besoins Métier par Département

Avant de construire un pipeline, il faut d'abord identifier les besoins spécifiques du client. J'ai supposé que nous avons identifié cela pour un client avec plusieurs départements, chaque département ayant des besoins distincts pour garantir que les données exposées répondent à leurs attentes.

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

### **Département Supply Chain & Logistique**

**Besoins métier :**
- Gestion des fournisseurs et des délais de livraison
- Analyse des niveaux de stock et des tendances de consommation
- Optimisation des commandes et des routes de livraison

**Métriques clés :**
- **Délai Moyen d'Expédition** (en jours) et sa variabilité
- **Taux de Livraisons Tardives (%)**
- **Taux d'Exécution des Commandes (%)**
- **Temps de Traitement Moyen** (en jours)
- **Nombre de Produits Uniques** et **Quantité Commandée**
- **Performance des Fournisseurs** par pays et région

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
- Construction des tables **prêtes pour l'analyse** (One Big Table ou tables KPI spécifiques).
- Calcul des **métriques clés** par département :
  - **Finance** : revenus, taxes, marges, créances
  - **Supply Chain** : délais de livraison, performance fournisseurs

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
git clone https://github.com/votre-username/tpch-etl-pipeline.git
cd tpch-etl-pipeline

# Démarrer l'environnement Docker
make up
```

### **2. Exécution du Pipeline**

Plusieurs options sont disponibles pour exécuter le pipeline :

```bash
# Exécuter uniquement la couche interface (par défaut)
make run-pipeline

# Exécuter uniquement les vues pour le département Finance
make run-finance

# Exécuter uniquement les vues pour le département Supply Chain
make run-supply-chain

# Exécuter toutes les couches (bronze, silver, gold, interface)
make run-all
```

### **3. Visualisation avec Apache Superset**

```bash
# Démarrer Superset
make run-superset
```

Accéder à l'interface web de Superset :
```
URL: http://localhost:8088
Identifiant: admin
Mot de passe: admin123
```

Pour configurer une connexion à Delta Lake dans Superset :
1. Aller dans "Data" > "Databases" > "+ Database"
2. Sélectionner "Trino" ou "Spark SQL" comme type de base de données
3. Configurer la connexion avec les paramètres appropriés
4. Créer des tableaux de bord pour visualiser les métriques clés

### **4. Autres Commandes Utiles**

```bash
# Démarrer un notebook Jupyter
make notebook

# Exécuter les tests
make test

# Vérifier le code avec Ruff
make check

# Formater le code avec Ruff
make format
```

---

## **Vues Disponibles par Département**

### **Finance & Comptabilité**

| Nom de la Vue | Description | Tables/Vues |
|---------------|-------------|-------------|
| **Finance Dashboard** | Tableau de bord financier avec les métriques clés | `tpchdb.finance_dashboard_view` |

### **Supply Chain & Logistique**

| Nom de la Vue | Description | Tables/Vues |
|---------------|-------------|-------------|
| **Supply Chain Dashboard** | Tableau de bord logistique avec les métriques clés | `tpchdb.supply_chain_dashboard_view` |
| **Supplier Performance** | Analyse de la performance des fournisseurs | `tpchdb.supplier_performance_view` |
| **Inventory Analysis** | Analyse des stocks et de la consommation | `tpchdb.inventory_analysis_view` |

---

## **Prochaines Étapes (Optionnel)**

- Ajout de métriques supplémentaires selon les besoins des départements
- Intégration avec d'autres outils de visualisation (Tableau, Power BI)
- Mise en place d'un orchestrateur (Airflow) pour automatiser les exécutions

En cours de dev
