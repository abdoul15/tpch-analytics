#!/bin/bash
set -e

# Créer le répertoire pour le metastore Hive si nécessaire
mkdir -p /tmp/trino/hive-metastore

# Utiliser sed pour remplacer les variables d'environnement dans le fichier delta.properties
cp /etc/trino/catalog/delta.properties.template /etc/trino/catalog/delta.properties
sed -i "s|\${MINIO_ACCESS_KEY}|$MINIO_ACCESS_KEY|g" /etc/trino/catalog/delta.properties
sed -i "s|\${MINIO_SECRET_KEY}|$MINIO_SECRET_KEY|g" /etc/trino/catalog/delta.properties

# Exécuter la commande par défaut de l'image Trino
exec /usr/lib/trino/bin/run-trino
