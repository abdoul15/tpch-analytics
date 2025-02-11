# Projet d'Analyse de Performance Supply Chain avec TPC-H

## 📚 Contexte
Ce projet vise à construire une architecture data moderne pour analyser les performances de la supply chain en utilisant le benchmark TPC-H. L'objectif est de fournir des insights précieux sur les opérations, les performances des fournisseurs, et le comportement des clients à travers une architecture en couches (Bronze, Silver, Gold, Interface).

Ce projet est conçu pour simuler un environnement d'entreprise réel, en suivant les meilleures pratiques de l'industrie :
- Architecture en couches (médaillon) similaire à celle utilisée par de grandes entreprises
- Gestion des données inspirée des pratiques réelles de supply chain
- Implémentation des standards de qualité et de tests comme en entreprise
- Documentation exhaustive comme attendu en environnement professionnel
- Processus de versioning et de contribution calqué sur les pratiques DevOps actuelles

## 🎯 Objectifs Métier

### Analyse des Besoins Stakeholders

#### 1. Business Impact
- Optimisation des coûts d'approvisionnement
- Amélioration de la satisfaction client
- Identification des opportunités d'amélioration de la performance fournisseur
- Optimisation de la gestion des stocks
- Amélioration des marges opérationnelles

#### 2. Compréhension Sémantique
Les données représentent l'ensemble des opérations de la chaîne d'approvisionnement :
- Commandes clients
- Gestion des stocks
- Relations fournisseurs
- Tarification
- Performance logistique

#### 3. Source de Données
- Source principale : TPC-H Data Generator
- Format : Tables relationnelles
- Volume : Scalable (SF: Scale Factor configurable)
- Tables principales : ORDERS, LINEITEM, CUSTOMER, SUPPLIER, PART, etc.

#### 4. Fréquence de Mise à Jour
- Retraitement complet du dataset à chaque exécution
- Exécution quotidienne du pipeline complet
- Conservation des versions via horodatage (etl_inserted_at)
- Pas de traitement incrémental
- Avantages de cette approche :
  - Simplicité de maintenance
  - Facilité de détection des problèmes
  - Traçabilité complète
- Contraintes :
  - Temps de traitement plus long
  - Besoin en ressources plus important

#### 6. Particularités des Données
- Forte volumétrie sur LINEITEM et ORDERS
- Saisonnalité des commandes
- Relations complexes entre PART et SUPPLIER
- Possible retard dans les mises à jour de statut des commandes

#### 8. Contrôles Qualité
- Validation des clés primaires et étrangères
- Contrôle des montants (pas de valeurs négatives)
- Vérification des dates (cohérence chronologique)
- Monitoring des métriques clés :
  - Variation du volume de commandes
  - Écarts de prix
  - Taux de livraison à temps
  - Complétude des données

## 🎯 Livrables Attendus

### 1. Tables Analytiques
- Tables en couches (Bronze, Silver, Gold, Interface)
- Tests de qualité des données

### 2. Requêtes SQL Optimisées
- Requêtes pour les KPIs principaux
- Analyses prédéfinies
- Documentation des requêtes

## 🛠️ Prérequis Techniques
- Apache Spark 3.x
- Delta Lake
- Minio 
- Python 3.8+
- Git
- TPC-H Data Generator
