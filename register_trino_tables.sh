#!/bin/bash
# Script pour enregistrer les tables Delta dans Trino

# Créer le schéma finance s'il n'existe pas
echo "Création du schéma finance dans Trino..."
docker exec -i trino trino --catalog delta --execute "CREATE SCHEMA IF NOT EXISTS finance"

# Enregistrer la table finance_dashboard_view
echo "Enregistrement de la table finance.finance_dashboard_view dans Trino..."
docker exec -i trino trino --catalog delta --execute "
CALL system.register_table('finance', 'finance_dashboard_view', 's3://spark-bucket/delta/interface/finance/dashboard')
"

echo "Table finance.finance_dashboard_view enregistrée avec succès dans Trino!"
