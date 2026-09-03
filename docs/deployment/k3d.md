# k3d Cluster Setup & Architecture

This document details the configuration, creation, and networking setup for the local `bigdata-dev` Kubernetes cluster using **k3d**.

---

## 1. Cluster Specification

The cluster configuration is defined in [`infra/k3d/config.yaml`](../../infra/k3d/config.yaml):

- **Cluster Name**: `bigdata-dev`
- **Servers (Control Plane)**: `1`
- **Agents (Workers)**: `2`
- **Image**: `rancher/k3s:v1.31.2-k3s1`
- **Storage Volume**: Named Docker volume `k3d-storage:/var/lib/rancher/k3s/storage@all`

### Port Mappings (Host -> LoadBalancer)

| Host Port | Node / Container Port | Target Service | Protocol |
| :--- | :--- | :--- | :--- |
| `5432` | `5432` | PostgreSQL (Hive Metastore DB) | TCP |
| `9000` | `9000` | MinIO S3 API | HTTP |
| `9001` | `9001` | MinIO Console Web UI | HTTP |
| `9083` | `9083` | Apache Hive Metastore Thrift | TCP / Thrift |
| `4040` | `4040` | Apache Spark Driver Web UI | HTTP |

---

## 2. Cluster Creation Commands

### Via Makefile:
```bash
make up
```

### Via CLI:
```bash
k3d cluster create --config infra/k3d/config.yaml
```

---

## 3. Windows & WSL2 Host Connectivity Setup

When creating a k3d cluster on Windows, k3d writes `https://host.docker.internal:<port>` into `~/.kube/config`. On Windows PowerShell, `host.docker.internal` may not resolve to `localhost` properly, leading to `dial tcp: timeout` errors.

### Automated Fix:
The cluster startup script automatically detects the cluster port and configures the kubeconfig context:
```powershell
$clusterPort = (kubectl config view -o jsonpath='{.clusters[?(@.name=="k3d-bigdata-dev")].cluster.server}').Split(':')[-1]
kubectl config set-cluster k3d-bigdata-dev --server="https://127.0.0.1:$clusterPort"
```

---

## 4. Deletion & Reset

To cleanly tear down the cluster and release all allocated container ports:
```bash
make down
# or
k3d cluster delete bigdata-dev
```
