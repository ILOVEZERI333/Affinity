# Phase 3 — Control plane (where it sits and what it does)

Phase 3 is **only the Kubernetes control plane**: you declare a `DistributedScraper` CR; the **operator** turns that into real cluster objects and writes **observed** state back on the CR. It does **not** run Raft, the hash ring, scraping, or webhooks yet—that is Phases 4–8 on the workers.

---

## Big picture: two planes

```
┌─────────────────────────────────── PHASE 3 — CONTROL PLANE ───────────────────────────────────┐
│                                                                                                │
│   examples/manhwa-tracker.yaml                                                                 │
│              │                                                                                 │
│              ▼  (you apply desired state)                                                       │
│   api/v1/distributedscraper_types.go  ◄── Spec + Status schema                                 │
│              ▲                                                                                 │
│              │  status.phase, readyWorkers, WorkersReady                                       │
│              │                                                                                 │
│   cmd/operator/main.go                                                                         │
│              │  starts manager, registers reconciler                                           │
│              ▼                                                                                 │
│   internal/controller/distributedscraper_controller.go                                         │
│              │  Reconcile loop                                                                 │
│              ├──────────────────────────────┐                                                    │
│              ▼                              ▼                                                    │
│   internal/controller/resources.go    Kubernetes API (create/update)                           │
│              │                              │                                                    │
│              │ builds desired objects       ├── ConfigMap      {name}-targets                  │
│              └──────────────────────────────├── Service        {name}-workers (headless :7946) │
│                                             └── StatefulSet    {name}-workers                  │
│                                                                                                │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
                                              │
                                              │ StatefulSet creates pods + env
                                              ▼
┌────────────────────────────────── LATER — DATA PLANE (Phases 4–8) ─────────────────────────────┐
│                                                                                                │
│   cmd/worker/main.go  ──►  internal/worker/raft.go                                             │
│                        ──►  internal/worker/hashring.go                                        │
│                        ──►  internal/scraper/engine.go                                         │
│                                                                                                │
│   ConfigMap targets.yaml  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  │
│   (object exists in Phase 3; mount/read on worker in Phase 4+)                                 │
│                                                                                                │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Takeaway:** Phase 3 answers: *“Did Kubernetes provision the worker fleet and report readiness on the CR?”* It does not answer: *“Is the cluster scraping and posting to Discord?”*

---

## Apply → reconcile → status (one CR)

```
  You                Kubernetes API          cmd/operator/main.go
   │                        │                         │
   │  kubectl apply         │                         │
   │  examples/manhwa-      │                         │
   │  tracker.yaml          │                         │
   ├───────────────────────►│                         │
   │                        │  watch: CR changed      │
   │                        ├────────────────────────►│
   │                        │                         │
   │                        │     distributedscraper_controller.go
   │                        │                         │
   │                        │◄──── Get(DistributedScraper)
   │                        │                         │
   │                        │     resources.go
   │                        │     (desired ConfigMap, Service, StatefulSet)
   │                        │                         │
   │                        │◄──── Create/Update CM, SVC, STS
   │                        │      (owner refs → delete CR cleans up children)
   │                        │                         │
   │                        │     Read StatefulSet.status.readyReplicas
   │                        │                         │
   │                        │◄──── Status().Update
   │                        │      phase, readyWorkers, WorkersReady
   │                        │                         │
   │                        │     if readyWorkers < workerNodes:
   │                        │         RequeueAfter 30s
   │                        │                         │
```

---

## Files and responsibilities

| File | Role in Phase 3 |
| --- | --- |
| `examples/manhwa-tracker.yaml` | Sample **desired** state you apply (`targetList`, `workerNodes`, `syncInterval`, `webhookDestination`). |
| `api/v1/distributedscraper_types.go` | **Contract**: `Spec` = what you want; `Status` = what the operator observed (`phase`, `readyWorkers`, `conditions`). Types only—no reconcile logic. |
| `api/v1/groupversion_info.go` | API group `infra.tony/v1` (CRD identity). |
| `cmd/operator/main.go` | Process entry: registers scheme, starts controller-runtime **manager**, registers `DistributedScraperReconciler`. |
| `internal/controller/distributedscraper_controller.go` | **Reconcile loop**: get CR → ensure children → read StatefulSet readiness → patch status → requeue if not all ready. Watches owned STS/SVC/CM. |
| `internal/controller/resources.go` | **Pure builders**: names, labels, ConfigMap data, headless Service port 7946, StatefulSet pod template (image, env, `emptyDir` for `/data/raft`). |
| `internal/controller/distributedscraper_controller_test.go` | Unit tests on desired objects (naming, targets file, headless service, replicas/env)—no cluster required. |
| `cmd/worker/main.go` | **Started by Phase 3** (pods run) but **data-plane behavior is mostly stub/TODO**; reads env the operator sets. `bootstrapFromEnv()` picks ordinal 0 via pod name `-0`. |
| `Dockerfile.worker` / `Dockerfile.operator` | Images referenced as `distriscrape-worker:latest` (StatefulSet) and operator image for deploy. |
| `docs/ARCHITECTURE_PHASES.md` | Master roadmap; Phase 3 marked ✅ complete with blueprint details. |

---

## Kubernetes objects the operator owns

For CR `metadata.name: manhwa-tracker` in namespace `default`:

```
                    DistributedScraper
                    (manhwa-tracker)
                           │
           ownerReference  │  ownerReference  │  ownerReference
                 ┌─────────┼─────────┬────────┴────────┐
                 ▼         ▼         ▼                 │
          ConfigMap    Service   StatefulSet            │
    manhwa-tracker-   manhwa-    manhwa-tracker-       │
       targets        tracker-      workers             │
                      workers      replicas=3            │
                 │         │         │                 │
                 │         │         ├────► Pod manhwa-tracker-workers-0
                 │         │         ├────► Pod manhwa-tracker-workers-1
                 │         │         └────► Pod manhwa-tracker-workers-2
                 │         │              (headless SVC gives stable DNS per pod)
                 │         └──────────────► selects same pod labels
                 │
                 └── data: targets.yaml (target IDs, one per line)
```

| Child | Built in | Contents / behavior |
| --- | --- | --- |
| ConfigMap | `resources.go` → `desiredConfigMap` | Key `targets.yaml`: one target ID per line from `spec.targetList`. **Not mounted on pods yet**—object exists for Phase 4+. |
| Service | `desiredService` | Headless DNS for future peer discovery; port **7946** matches worker `BIND_ADDR`. |
| StatefulSet | `desiredStatefulSet` | Image `distriscrape-worker:latest`, `emptyDir` at `/data/raft`, env from spec (`SYNC_INTERVAL`, `WEBHOOK_URL`, downward API `POD_NAME` / `NODE_ID`). |

Labels on all children: `app.kubernetes.io/name=distriscrape-worker`, `app.kubernetes.io/instance=<CR name>` (used in docs for `kubectl get ... -l`).

---

## Status fields Phase 3 actually sets

From `distributedscraper_controller.go` + types in `distributedscraper_types.go`:

| Field / condition | Phase 3 behavior |
| --- | --- |
| `status.phase` | `Provisioning` during reconcile; `Running` once StatefulSet reconcile succeeded (pods may still be starting). |
| `status.readyWorkers` | Copied from StatefulSet `.status.readyReplicas`. |
| `WorkersReady` condition | `True` when `readyWorkers >= spec.workerNodes`. |
| `status.leaderNode` | **Not set** (Phase 5). |
| `LeaderElected` | **Not set** (Phase 5). |
| `lastSyncTime`, `TargetsAssigned` | **Not set** (later phases). |

**Proof of done for this phase:** CR shows `Running` and ready count matches `workerNodes`; pods exist and stay up. Raft clustering and scraping correctness are out of scope until later phases.

---

## What flows from Spec → pods (env wiring)

```
api/v1/distributedscraper_types.go (Spec)
─────────────────────────────────────
  targetList ──────────────► ConfigMap key targets.yaml  (not mounted yet)
  workerNodes ─────────────► StatefulSet.spec.replicas
  syncInterval ────────────► pod env SYNC_INTERVAL
  webhookDestination ──────► pod env WEBHOOK_URL

StatefulSet pod template (resources.go) + cmd/worker/main.go (loadConfig)
────────────────────────────────────────────────────────────────────────
  Kubernetes downward API ─► POD_NAME, NODE_ID
  literal / defaults      ─► BIND_ADDR=:7946, DATA_DIR=/data/raft
  emptyDir volume         ─► /data/raft on disk
  pod name ends with -0   ─► bootstrapFromEnv() → Raft bootstrap (ordinal 0)

  PEER_ADDRS              ─► (empty until Phase 4)
```

---

## Phase 3 vs what comes next

| Concern | Phase 3 | Next (Phase 4+) |
| --- | --- | --- |
| Provision workers | ✅ StatefulSet + Service + ConfigMap | PVC for Raft dir, mount `targets.yaml` |
| Report readiness on CR | ✅ `readyWorkers`, `WorkersReady` | Add `leaderNode`, `LeaderElected` when Raft works |
| Cluster membership | Pods run; Raft **stub** in `internal/worker/raft.go` | Real bootstrap + `PEER_ADDRS` / DNS |
| Work placement / scrape / webhook | Env passed through; engine runs loop but placement is TODO | Hash ring, handoff, real delivery + outbox |

---

## Quick commands (mental model check)

```bash
kubectl apply -f examples/manhwa-tracker.yaml
kubectl get distributedscraper manhwa-tracker    # Phase, Ready columns from CRD printcolumns
kubectl get sts,svc,cm -l app.kubernetes.io/instance=manhwa-tracker
kubectl get pods -l app.kubernetes.io/instance=manhwa-tracker
```

When you can trace **YAML → three owned resources → pods → CR status** without opening Raft code, you understand Phase 3.
