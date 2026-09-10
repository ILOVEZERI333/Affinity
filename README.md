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

Apply a `DistributedScraper` once the operator is running in-cluster; the reconciler will stand up the worker StatefulSet and related resources from `spec`.
