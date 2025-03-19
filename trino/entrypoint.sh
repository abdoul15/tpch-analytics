#!/bin/bash
set -e

# Créer le répertoire pour le metastore Hive si nécessaire
mkdir -p /tmp/trino/hive-metastore

# Installer gettext qui contient envsubst
apt-get update && apt-get install -y gettext-base

# Remplacer les variables d'environnement dans le fichier delta.properties
envsubst < /etc/trino/catalog/delta.properties.template > /etc/trino/catalog/delta.properties

# Exécuter la commande par défaut de l'image Trino
exec /usr/lib/trino/bin/run-trino
