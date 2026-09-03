#!/usr/bin/env bash
# scripts/init/01-init-minio-buckets.sh
# Creates standard Data Lakehouse buckets and access policies in MinIO

set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-miniopassword}"

echo "Step 1: Initializing MinIO Buckets on ${MINIO_ENDPOINT}"

BUCKETS=(
  "lakehouse"
  "raw"
  "processed"
  "curated"
  "checkpoints"
  "spark-logs"
  "warehouse"
)

if kubectl get pod minio-0 -n minio >/dev/null 2>&1; then
  echo "Using mc inside minio-0 pod"
  kubectl exec -n minio minio-0 -- mc alias set local http://localhost:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null

  for b in "${BUCKETS[@]}"; do
    if kubectl exec -n minio minio-0 -- mc ls "local/${b}" >/dev/null 2>&1; then
      echo "Bucket local/${b} already exists"
    else
      kubectl exec -n minio minio-0 -- mc mb "local/${b}"
      echo "Created bucket: ${b}"
    fi
  done
else
  echo "Warning: minio-0 pod not found. Trying local mc if installed"
  if command -v mc >/dev/null 2>&1; then
    mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null
    for b in "${BUCKETS[@]}"; do
      mc mb --ignore-existing "local/${b}"
      echo "Created bucket: ${b}"
    done
  else
    echo "Notice: mc not available locally. Bucket init job in K8s handled this automatically"
  fi
fi

echo "MinIO buckets initialized successfully"
