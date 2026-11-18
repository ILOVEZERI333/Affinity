# Affinity
 a Kubernetes operator and distributed worker pool for tracking manga/manhwa drops and delivering chapter updates to Discord webhooks.

Developers declare *what* to track. Affinity provisions the infrastructure and runs a clustered backend that shards work, elects a leader, and stays available when nodes die.

---

## Architecture

Two pieces of software talk to each other:

### 1. Platform side — the Kubernetes Operator

Hides servers, networking, and scaling behind a Custom Resource. Built with Go and Kubebuilder, it extends the Kubernetes API with a `DistributedScraper` CRD.

A developer submits YAML like:

```yaml
apiVersion: infra.tony/v1
kind: DistributedScraper
metadata:
  name: manhwa-tracker
spec:
  targetList:
    - solo-leveling
    - omniscient-reader
    - tower-of-god
  workerNodes: 3
  syncInterval: "30s"
  webhookDestination: "discord-webhook-url"
```

**Operator responsibilities**

- Watch the API for `DistributedScraper` objects
- Reconcile desired state into Kubernetes primitives: Headless Service, StatefulSet (workers), and ConfigMaps (targets)
- Detect drift (e.g. `workerNodes: 3` → `5`) and scale the worker pool automatically

### 2. Infrastructure side — the Worker Pool

Workers are not dumb containers. They form a clustered backend with shared state and sharded scrape work.

**Worker responsibilities**

| Concern | Behavior |
| --- | --- |
| **Leader election** | Nodes use Raft (HashiCorp `raft`) to elect a leader |
| **Consistent hashing** | The leader places `targetList` on a hash ring and assigns URLs to nodes so no two scrape the same target at once |
| **Fault tolerance** | On node loss, the leader updates the ring and reassigns that node's URLs to survivors |
| **Delivery** | On a new chapter, the worker posts to `webhookDestination` |

---

## Repository layout

```
.
├── api/v1/                          # CRD types (DistributedScraper)
├── cmd/
│   ├── operator/                    # Operator entrypoint
│   └── worker/                      # Worker node entrypoint
├── config/                          # Kustomize manifests (CRD, manager, RBAC)
├── internal/
│   ├── controller/                  # Reconciliation loop
│   ├── worker/                      # Raft, consistent hash ring, peer server
│   └── scraper/                     # Manga tracking / scrape engine
├── Dockerfile.operator
├── Dockerfile.worker
├── Makefile
└── go.mod
```

---

## Quick start

```bash
make build          # bin/operator, bin/worker
make test
make docker-build   # operator + worker images
```

### Run it locally on kind

Requires `docker`, `kubectl` and `kind`.

```bash
make docker-build

kind create cluster --name affinity
kind load docker-image distriscrape-operator:latest distriscrape-worker:latest --name affinity

# CRD, RBAC, then the operator Deployment (all in the default namespace)
kubectl apply -k config/crd
kubectl apply -k config/rbac
kubectl apply -k config/manager
kubectl rollout status deploy/affinity-operator

# Declare what to track; the operator creates the worker StatefulSet, Service and ConfigMap
kubectl apply -f examples/manhwa-tracker.yaml
kubectl get distributedscraper,sts,pods

# Scale
kubectl patch distributedscraper manhwa-tracker --type merge -p '{"spec":{"workerNodes":5}}'

kind delete cluster --name affinity   # clean up
```

The example's `webhookDestination` is a placeholder and no chapter source is configured, so workers will log `poll failed`. The operator-managed worker template has no supported way to set `SOURCE_URL`, so setting `webhookDestination` alone will not enable polling, and hand-editing the StatefulSet is overwritten on the next reconcile. The CRD and RBAC manifests in `config/` are generated with controller-gen from the markers in `api/` and `internal/controller/`.

### Run workers without Kubernetes

Workers are plain processes configured by env vars (`NODE_ID`, `BIND_ADDR`, `API_BIND_ADDR`, `HEALTH_ADDR`, `PEER_ADDRS`, `DATA_DIR`, `TARGETS_FILE`, `SOURCE_URL`, `WEBHOOK_URL`, `BOOTSTRAP`). Start one with `BOOTSTRAP=true` and point the rest at it via `PEER_ADDRS`. When workers run on different hosts, set `ADVERTISE_ADDR` on each to an address the other hosts can reach. Workers sharing a host need unique `BIND_ADDR`, `API_BIND_ADDR`, `HEALTH_ADDR` and `DATA_DIR` values to avoid listener conflicts and shared state. `SOURCE_URL` must return a JSON array of `{"number", "title", "url"}`, with `{target}` substituted per series.
