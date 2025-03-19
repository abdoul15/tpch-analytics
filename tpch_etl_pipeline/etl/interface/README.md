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
