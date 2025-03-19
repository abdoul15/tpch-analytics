#!/bin/bash

echo "En attente que PostgreSQL soit prêt..."
export PGPASSWORD=${POSTGRES_PASSWORD}
until psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c '\l' > /dev/null 2>&1; do
  echo "Tentative de connexion à PostgreSQL..."
  sleep 1
done
echo "PostgreSQL est prêt!"

# Creation du répertoire temporaire dans /tmp
TEMP_DIR="/tmp/tpch_load"
mkdir -p $TEMP_DIR

# Copie et nettoyer les fichiers
for file in /opt/tpch-data/*.tbl; do
    filename=$(basename $file)
    # Suppression le délimiteur de fin et nettoyer le fichier
    sed 's/|$//' "$file" > "$TEMP_DIR/$filename"
done

echo "Chargement des données TPC-H dans PostgreSQL..."
psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<-EOSQL
    TRUNCATE TABLE lineitem, orders, customer, partsupp, supplier, part, nation, region CASCADE;
    
    \copy nation FROM '$TEMP_DIR/nation.tbl' DELIMITER '|' NULL AS '';
    \copy region FROM '$TEMP_DIR/region.tbl' DELIMITER '|' NULL AS '';
    \copy part FROM '$TEMP_DIR/part.tbl' DELIMITER '|' NULL AS '';
    \copy supplier FROM '$TEMP_DIR/supplier.tbl' DELIMITER '|' NULL AS '';
    \copy partsupp FROM '$TEMP_DIR/partsupp.tbl' DELIMITER '|' NULL AS '';
    
    \copy customer FROM '$TEMP_DIR/customer.tbl' DELIMITER '|' NULL AS '';
    \copy orders FROM '$TEMP_DIR/orders.tbl' DELIMITER '|' NULL AS '';
    \copy lineitem FROM '$TEMP_DIR/lineitem.tbl' DELIMITER '|' NULL AS '';
EOSQL

rm -rf $TEMP_DIR

echo "Chargement des données terminé avec succès!"