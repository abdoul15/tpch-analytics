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