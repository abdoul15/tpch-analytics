WORKER_COUNT=2

init-scripts:
	chmod +x postgres/load-tpch.sh

build: init-scripts
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=$(WORKER_COUNT)

up: build docker-up

down:
	docker compose down -v

rebuild:
	docker compose build
	docker compose up -d --scale spark-worker=$(WORKER_COUNT)



notebook:
	docker exec spark-master bash -c "/opt/spark/scripts/start-jupyter.sh"

# Vérifier le code avec Ruff
check:
	docker exec spark-master ruff check .

# Formater le code avec Ruff
format:
	docker exec spark-master ruff format .

# Corriger automatiquement les problèmes avec Ruff
fix:
	docker exec spark-master ruff check --fix .

spark-sql:
	docker exec -ti spark-master spark-sql --master spark://spark-master:7077

cr: 
	@read -p "Entrer le chemin du fichier :" pyspark_path; docker exec -ti spark-master spark-submit --master spark://spark-master:7077 /opt/spark/project/$$pyspark_path

# Exécuter le pipeline complet
run-pipeline:
	docker exec -ti spark-master spark-submit --master spark://spark-master:7077 /opt/spark/project/tpch_etl_pipeline/run_pipeline.py

# Exécuter uniquement les vues pour le département Finance
run-finance:
	docker exec -ti spark-master spark-submit --master spark://spark-master:7077 /opt/spark/project/tpch_etl_pipeline/run_pipeline.py finance
	

# Enregistrer les tables Delta dans Trino
register-trino-tables:
	./register_trino_tables.sh

# Exécuter tous les tests
test:
	docker exec spark-master pytest -xvs /opt/spark/project/tpch_etl_pipeline/tests

# Exécuter uniquement les tests unitaires
test-unit:
	docker exec spark-master pytest -xvs /opt/spark/project/tpch_etl_pipeline/tests/unit

# Exécuter uniquement les tests d'intégration
test-integration:
	docker exec spark-master pytest -xvs /opt/spark/project/tpch_etl_pipeline/tests/integration

# Exécuter les tests avec couverture de code
test-coverage:
	docker exec spark-master pytest --cov=tpch_etl_pipeline /opt/spark/project/tpch_etl_pipeline/tests

# Great Expectations commands
# Initialiser Great Expectations
ge-init:
	docker exec spark-master bash -c "cd /opt/spark/project/tpch_etl_pipeline && great_expectations init"

# Générer la documentation des attentes
ge-docs:
	docker exec -ti spark-master bash -c "cd /opt/spark/project/tpch_etl_pipeline && great_expectations docs build"
