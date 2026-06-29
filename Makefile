# NYC Taxi Analytics — Infrastructure Makefile

.PHONY: up down build logs status clean shell spark-shell

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

build:
	docker compose -f docker/docker-compose.yml build

logs:
	docker compose -f docker/docker-compose.yml logs -f

status:
	docker compose -f docker/docker-compose.yml ps

clean:
	docker compose -f docker/docker-compose.yml down -v
	rm -rf airflow/logs/*
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*

shell-airflow:
	docker exec -it airflow-scheduler bash

shell-spark:
	docker exec -it spark-master bash

run-capture:
	powershell -ExecutionPolicy Bypass -File scripts/run_and_capture.ps1

run-capture-fault:
	powershell -ExecutionPolicy Bypass -File scripts/run_and_capture.ps1 -FaultInject
