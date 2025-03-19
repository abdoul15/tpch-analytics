#!/bin/bash
set -e

echo "En attente que PostgreSQL soit prêt..."
export PGPASSWORD=${POSTGRES_PASSWORD}
until psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c '\l' > /dev/null 2>&1; do
  echo "Tentative de connexion à PostgreSQL..."
  sleep 1
done
echo "PostgreSQL est prêt!"

# Création du répertoire temporaire dans /var/lib/postgresql qui est garanti accessible
TEMP_DIR="/var/lib/postgresql/tpch_load"
mkdir -p $TEMP_DIR
chmod 777 $TEMP_DIR

echo "Chargement des données TPC-H dans PostgreSQL..."

# Fonction pour charger une table individuelle avec gestion d'erreur
load_table() {
    local table=$1
    local file="/opt/tpch-data/${table}.tbl"
    local temp_file="${TEMP_DIR}/${table}.tbl"
    
    echo "Traitement de la table $table..."
    
    if [ ! -f "$file" ]; then
        echo "AVERTISSEMENT: Le fichier $file n'existe pas. Création d'un fichier vide."
        touch "$temp_file"
    else

        if ! sed 's/|$//' "$file" > "$temp_file" 2>/dev/null; then
            echo "ERREUR: Impossible de traiter $file. Création d'un fichier vide."
            touch "$temp_file"
        fi
    fi
    
    # Chargement des données dans la table
    echo "Chargement de la table $table..."
    if ! psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "\copy $table FROM '$temp_file' DELIMITER '|' NULL AS '';" 2>/dev/null; then
        echo "AVERTISSEMENT: Échec du chargement de la table $table."
    else
        echo "Table $table chargée avec succès."
    fi
}

# Charge chaque table individuellement
load_table "nation"
load_table "region"
load_table "part"
load_table "supplier"
load_table "partsupp"
load_table "customer"
load_table "orders"
load_table "lineitem"

# Nettoyage
rm -rf $TEMP_DIR

echo "Processus de chargement des données terminé!"
