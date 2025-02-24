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
	@read -p "Enter pyspark relative path:" pyspark_path; docker exec -ti spark-master spark-submit --master spark://spark-master:7077 /opt/spark/project/$$pyspark_path