# Ajouter dans votre Makefile existant
init-scripts:
	chmod +x postgres/load-tpch.sh

build: init-scripts
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=2

up: build docker-up

down:
	docker-compose down -v

rebuild:
	docker-compose build
	docker-compose up -d


#test-minio:
#	docker exec spark-master python3 /opt/spark/tests/test-minio.py


notebook:
notebook:
	docker exec spark-master bash -c "/opt/spark/scripts/start-jupyter.sh"