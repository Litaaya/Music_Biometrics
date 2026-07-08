COMPOSE=docker compose

.PHONY: build up infra bootstrap app worker generator airflow down clean logs ps restart

build:
	$(COMPOSE) build

infra:
	$(COMPOSE) up -d minio catalog-db minio-init iceberg-rest kafka schema-registry

bootstrap:
	$(COMPOSE) run --rm bootstrap-loader

app:
	$(COMPOSE) up -d spark-streaming-worker biometric-generator

worker:
	$(COMPOSE) up -d spark-streaming-worker

generator:
	$(COMPOSE) up -d biometric-generator

airflow:
	$(COMPOSE) up -d airflow

up: build infra bootstrap app
	@echo "Platform is running"

restart:
	$(COMPOSE) down
	$(MAKE) up

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

lakehouse:
	$(COMPOSE) up -d minio catalog-db minio-init iceberg-rest