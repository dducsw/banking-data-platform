.PHONY: help up down deploy-lakehouse deploy-helm deploy-airflow init-data build-spark import-spark build-airflow import-airflow build-all import-all pull-images import-images preload-all status logs-postgres logs-minio logs-gravitino logs-airflow logs-clickhouse logs-trino logs-prometheus logs-kafka logs-debezium cli-clickhouse cli-trino forward-prometheus forward-debezium run-job-postgres run-job-iceberg

SPARK_IMAGE := banking/spark:4.1
AIRFLOW_IMAGE := banking/airflow:3.1.3
CLUSTER_NAME := bigdata-dev

THIRD_PARTY_IMAGES := postgres:16-alpine \
                      minio/minio:latest \
                      apache/gravitino:1.3.0 \
                      trinodb/trino:480 \
                      apache/kafka:4.1.0 \
                      quay.io/debezium/connect:3.2.0.Final \
                      prom/prometheus:v3.14.0 \
                      clickhouse/clickhouse-server:24.3-alpine \
                      busybox:1.36

help:
	@echo "Big Data Platform Infrastructure Management"
	@echo "make up               : Create k3d cluster and deploy Lakehouse Dev Stack"
	@echo "make down             : Delete k3d cluster and local state"
	@echo "make pull-images      : Pre-pull all third-party container images"
	@echo "make import-images    : Import all third-party container images into k3d"
	@echo "make build-all        : Build all custom Docker images (Spark, Airflow)"
	@echo "make import-all       : Import all Docker images (custom + third-party) into k3d"
	@echo "make preload-all      : Full pipeline: pull images, build custom, and import all into k3d"
	@echo "make deploy-lakehouse : Apply Kustomize Dev Overlay"
	@echo "make init-data        : Run all scripts in scripts/init/ (Buckets, DB DDL)"
	@echo "make status           : Show status of all pods across namespaces"
	@echo "make cli-trino        : Open interactive Trino SQL CLI"
	@echo "make cli-clickhouse   : Open interactive ClickHouse SQL CLI"

up:
	bash infra/k3d/cluster-dev.sh

down:
	k3d cluster delete $(CLUSTER_NAME) || true

pull-images:
	@for img in $(THIRD_PARTY_IMAGES); do \
		echo "Pulling $$img..."; \
		docker pull $$img; \
	done

import-images:
	@for img in $(THIRD_PARTY_IMAGES); do \
		echo "Importing $$img into $(CLUSTER_NAME)..."; \
		k3d image import $$img -c $(CLUSTER_NAME) || true; \
	done

build-all: build-spark build-airflow

import-all: import-spark import-airflow import-images

preload-all: pull-images build-all import-all

build-spark:
	docker build -t $(SPARK_IMAGE) infra/docker/spark

import-spark:
	k3d image import $(SPARK_IMAGE) -c $(CLUSTER_NAME)

build-airflow:
	docker build -t $(AIRFLOW_IMAGE) infra/docker/airflow

import-airflow:
	k3d image import $(AIRFLOW_IMAGE) -c $(CLUSTER_NAME)

deploy-airflow:
	kubectl apply -k infra/k8s/base/airflow

deploy-clickhouse:
	kubectl apply -k infra/k8s/base/clickhouse

deploy-lakehouse:
	kubectl apply -k infra/k8s/overlays/dev

deploy-helm:
	bash infra/helm/install-dev.sh

init-data:
	bash scripts/init/bootstrap.sh

status:
	kubectl get pods -A

logs-postgres:
	kubectl logs -n postgres -l app=postgres -f

logs-minio:
	kubectl logs -n minio -l app=minio -f

logs-gravitino:
	kubectl logs -n gravitino -l app=gravitino -f

logs-airflow:
	kubectl logs -n airflow -l app.kubernetes.io/name=airflow --tail=100 -f

logs-clickhouse:
	kubectl logs -n clickhouse -l app=clickhouse -f

cli-clickhouse:
	kubectl exec -it -n clickhouse clickhouse-0 -- clickhouse-client --user admin --password clickhouse123

logs-trino:
	kubectl logs -n trino -l app=trino -f

cli-trino:
	kubectl exec -it -n trino deploy/trino -- trino

logs-prometheus:
	kubectl logs -n monitoring -l app=prometheus -f

forward-prometheus:
	kubectl port-forward -n monitoring svc/prometheus 9090:9090

logs-kafka:
	kubectl logs -n kafka -l app=kafka -f

logs-debezium:
	kubectl logs -n debezium -l app=debezium-connect -f

forward-debezium:
	kubectl port-forward -n debezium svc/debezium-connect 8083:8083

run-job-postgres:
	kubectl create configmap spark-job-read-postgres --from-file=spark_read_postgres.py=scripts/example/spark_read_postgres.py -n spark --dry-run=client -o yaml | kubectl apply -f -
	kubectl delete job spark-read-postgres -n spark --ignore-not-found
	kubectl apply -f infra/k8s/jobs/job-spark-read-postgres.yaml
	kubectl wait --for=condition=complete job/spark-read-postgres -n spark --timeout=60s || true
	kubectl logs -n spark -l app=spark-read-postgres --tail=100

run-job-iceberg:
	kubectl create configmap spark-job-write-iceberg --from-file=spark_write_iceberg.py=scripts/example/spark_write_iceberg.py -n spark --dry-run=client -o yaml | kubectl apply -f -
	kubectl delete job spark-write-iceberg -n spark --ignore-not-found
	kubectl apply -f infra/k8s/jobs/job-spark-write-iceberg.yaml
	kubectl wait --for=condition=complete job/spark-write-iceberg -n spark --timeout=60s || true
	kubectl logs -n spark -l app=spark-write-iceberg --tail=100

run-job-bronze:
	kubectl create configmap spark-job-bronze-ingest --from-file=ingest_postgres.py=../pipeline/jobs/bronze/ingest_postgres.py -n spark --dry-run=client -o yaml | kubectl apply -f -
	kubectl delete job spark-bronze-ingest -n spark --ignore-not-found
	kubectl apply -f infra/k8s/jobs/job-spark-bronze-ingest.yaml
	kubectl wait --for=condition=complete job/spark-bronze-ingest -n spark --timeout=180s || true
	kubectl logs -n spark -l app=spark-bronze-ingest --tail=100
