# kube-applier-aws

`kube-applier-aws` is a per-management-cluster controller binary that runs on AWS and brokers
between Amazon DynamoDB and the local Kubernetes apiserver. It reads Desire documents
from DynamoDB and reconciles them against the cluster.

At a high level:
1. `ApplyDesire` — a kube manifest in `.spec.kubeContent` to server-side-apply.
   Success/failure is written to `.status.conditions["Successful"]`.
2. `DeleteDesire` — a kube item in `.spec.targetItem` to delete.
   Success/failure is written to `.status.conditions["Successful"]`.
3. `ReadDesire` — a kube item in `.spec.targetItem` to list/watch+inform on.
   The observed content is written to `.status.kubeContent`.
   Success/failure is written to `.status.conditions["Successful"]`.

## FIPS-compatible container build

`make image` and the CI image builds use `Containerfile`. The Red Hat Go
toolset builds a static binary with `CGO_ENABLED=0`, `GOFIPS140=certified`,
`GOEXPERIMENT=strictfipsruntime`, and the `no_openssl` build tag. This selects
Go's native FIPS cryptographic module without an OpenSSL runtime dependency.
The pinned builder currently resolves `certified` to module `v1.0.0`.

The pinned downstream toolchain compiles in `GODEBUG=fips140=auto`: FIPS mode
activates on a FIPS-enabled host, but is disabled on a non-FIPS host. Operators
can explicitly enable it with `GODEBUG=fips140=on`. A runtime `GODEBUG` override
takes precedence; `strictfipsruntime` rejects disabling FIPS mode on a
FIPS-enabled host. `GODEBUG=fips140=only` is intended for assessment and testing,
not production. Local `make build` uses the developer's Go toolchain and does
not apply these container-specific build settings.

See the [Go FIPS documentation](https://go.dev/doc/security/fips140) and the
[downstream Go toolchain documentation](https://github.com/golang-fips/go)
for module selection and runtime behavior. These build settings provide FIPS
compatibility; they do not certify this application or its deployment.

## Scale

The scale of the kube-applier is tiny: it covers a single management cluster.
A single management cluster will have low hundreds of HostedClusters and if we have about 100
\*Desires, we end up with about 10k \*Desires.
Ten thousand is such a small number that with simple poll and iterate at 50 qps, we can scan
every three minutes. The scale of a region is larger but is handled by DynamoDB so it will
scale far beyond our needs.

## API structure

The API types live in `internal/api/kubeapplier`.

Every `*Desire` API interacts with a single Kubernetes resource instance.
We do not support lists, label selection, or list-all.
This is for simplicity in reasoning about the status.

### ManagementCluster

Every `*Desire` API has a `.spec.managementCluster` field.
This is the name of the management cluster the `kube-applier` is running in.
It matches the value the binary was started with via `--management-cluster`.
Each management cluster has its own pair of DynamoDB table prefixes
(`mc-{clusterName}-specs` and `mc-{clusterName}-status`),
so the management cluster name determines which tables the binary connects to.

### Conditions

Each `*Desire` API has a list of conditions. One of those conditions is the `Successful` condition.
`Successful` is true if the operation succeeded:
1. For `ApplyDesire` — a successful server-side-apply.
2. For `DeleteDesire` — the item is no longer present in the cluster.
   This is NOT the same as the delete call succeeding; Kubernetes has finalizers.
3. For `ReadDesire` — the list/watch succeeded and the informer synced.

When the kube-apiserver call fails:
- `.status.conditions["Successful"].status` is `False`
- `.status.conditions["Successful"].reason` is `KubeAPIError`
- `.status.conditions["Successful"].message` is the error from the kube-apiserver.

When the kube-apiserver call cannot be executed:
- `.status.conditions["Successful"].status` is `False`
- `.status.conditions["Successful"].reason` is `PreCheckFailed`
- `.status.conditions["Successful"].message` describes what prevented the call.

## Database structure

Every management cluster has **six** DynamoDB tables with IAM-enforced directional isolation,
grouped into two prefixes:

| Prefix | Agent access | Backend access | Contents |
|---|---|---|---|
| `mc-{clusterName}-specs` | read-only | read-write | Spec documents written by the backend |
| `mc-{clusterName}-status` | read-write | read-only | Status documents written by the agent |

Each prefix has three tables (one per desire type):

```
mc-{managementClusterName}-specs-applydesires    (agent: read-only)
mc-{managementClusterName}-specs-deletedesires   (agent: read-only)
mc-{managementClusterName}-specs-readdesires     (agent: read-only)

mc-{managementClusterName}-status-applydesires   (agent: read-write)
mc-{managementClusterName}-status-deletedesires  (agent: read-write)
mc-{managementClusterName}-status-readdesires    (agent: read-write)
```

All tables use `documentID` (string) as the partition key. DynamoDB Streams
(`NEW_AND_OLD_IMAGES`) must be enabled on all specs tables so the informers can
receive real-time change notifications.

### Document IDs

Document IDs are deterministic UUID v5 values generated from:
```
uuid.NewSHA1(namespaceUUID, "{taskKey}/{group}/{version}/{resource}/{namespace}/{name}")
```
The namespace UUID is a fixed constant shared between the agent and backend
(defined in `internal/desireid`). The same document ID in both the specs and status
tables links a spec to its status.

### Optimistic concurrency

`Replace` uses a DynamoDB `ConditionExpression` (`version = :expected`) to enforce
optimistic concurrency. Each successful `Replace` increments the `version` counter.
If the document has changed since the last read, the write fails with
`ErrPreconditionFailed` and the controller retries with a fresh read.

### KubeContent encoding

`KubeContent` fields (`ApplyDesireSpec.KubeContent`, `ReadDesireStatus.KubeContent`)
use `dynamodbav:"-"` tags because the AWS SDK attributevalue codec cannot serialize
`runtime.RawExtension`. The CRUD layer handles these manually: on write, `RawExtension.Raw`
is stored as a JSON string in a separate top-level attribute (`spec_kubeContent` or
`status_kubeContent`); on read, the string is parsed back into `RawExtension.Raw`.

### DynamoDB metadata

Each desire type carries a `DynamoDBMetadata` struct with:
- `DocumentID` — the DynamoDB partition key (UUID v5); stored via explicit `PutItem`, not as a struct attribute
- `Version` — optimistic concurrency counter; incremented on every successful `Replace`
- `UpdateTime` — wall-clock timestamp set on `Create` and `Replace`
- `CreateTime` — wall-clock timestamp set on `Create` only

### Authentication and isolation

- **EKS Pod Identity / IRSA**: pod service account → IAM role (no long-lived credentials)
- Per-table IAM conditions enforce directional access:
  - specs tables: `dynamodb:GetItem`, `dynamodb:Scan`, `dynamodb:DescribeStream`, `dynamodb:GetRecords`, `dynamodb:GetShardIterator`, `dynamodb:ListStreams`
  - status tables: above plus `dynamodb:PutItem`, `dynamodb:DeleteItem`

### Go type details

The Go types live in `internal/database`.

`KubeApplierDBClient` is the two-prefix handle. It exposes read-only accessors for
the specs tables and full CRUD accessors for the status tables:
- `ApplyDesireSpecs() SpecReader[ApplyDesire]` — read-only, specs tables
- `DeleteDesireSpecs() SpecReader[DeleteDesire]` — read-only, specs tables
- `ReadDesireSpecs() SpecReader[ReadDesire]` — read-only, specs tables
- `ApplyDesireStatus() ResourceCRUD[ApplyDesire]` — read-write, status tables
- `DeleteDesireStatus() ResourceCRUD[DeleteDesire]` — read-write, status tables
- `ReadDesireStatus() ResourceCRUD[ReadDesire]` — read-write, status tables

`SpecReader[T]` provides read-only operations: `Get` and `List`.
`ResourceCRUD[T]` provides: `Get`, `List`, `Create`, `Replace`, `Delete`.

Sentinel errors — `ErrNotFound`, `ErrPreconditionFailed`, `ErrAlreadyExists` — are returned
by CRUD methods and tested with `IsNotFoundError`, `IsPreconditionFailedError`,
`IsAlreadyExistsError` helpers. There is no gRPC dependency.

Informers are constructed via `informers.NewKubeApplierInformers(specsClient, streamsClient, specsPrefix)`.
They watch the **specs tables only** via DynamoDB Streams. Each informer uses:
- An initial `Scan` (List) to populate the cache on startup
- A `dynamodbstreams` shard poller (Watch) to stream document changes in real time

A `listWatchWithoutWatchListSemantics` wrapper opts out of client-go's WatchList bookmark
protocol, which DynamoDB Streams does not support.

The `internal/database/informers`, `internal/database/listers`, and
`internal/database/listertesting` packages provide the informers and listers for the
`*Desire` APIs.

## Controller structure

The `kube-applier-aws` binary is controller-based. Instead of using a `Controller` type to
communicate `Degraded` status, that is communicated on the `*Desire`
`.status.conditions["Degraded"]` field.

Change detection uses `UpdateTime` comparison: a controller's `handleUpdate` only enqueues
work when `!oldD.UpdateTime.Equal(newD.UpdateTime)`. The field manager for server-side-apply
is `gcp-hcp-kube-applier`.

### ApplyDesireController

Uses the `ApplyDesire` informer to feed a sync function for `ApplyDesire` instances.
When the sync loop runs it:
1. Reads the spec from the specs table
2. Decodes `spec.kubeContent` into an unstructured object
3. Issues a server-side-apply with `force=true` via the dynamic client
4. Writes `Successful` / `Degraded` conditions and `appliedResourceGeneration` to the status table

**Adopting existing resources:** SSA's `force=true` claims field ownership over fields the
kube-applier writes even if a different field manager owned them previously, but it does not
delete fields the prior owner wrote that are no longer in the object. Adoption is handled
case-by-case rather than baked into the controller.

This controller resyncs every 10 minutes (cooldown-gated).

### DeleteDesireController

Uses the `DeleteDesire` informer to feed a sync function for `DeleteDesire` instances.
When the sync loop runs it:
1. Gets the `.spec.targetItem` from the cluster
   - If not found → writes `Successful=True` and returns
   - If found with a `deletionTimestamp` → writes `Successful=False / WaitingForDeletion` and returns
   - If found without a `deletionTimestamp` → issues a delete, then re-gets to check for finalizers
2. Writes conditions to the status table

This controller resyncs every 60 seconds (to poll finalizer completion).

### ReadDesireKubernetesController

One instance is created per `ReadDesire`. Each instance holds:
1. The `.spec.targetItem`
2. A single-item Kubernetes informer and lister
3. A `KubeApplierDBClient` reference and the desire's document ID

When the sync loop runs it reads the item from the Kubernetes lister and from the
`ReadDesireLister`, compares `.status.kubeContent` against the live object, and writes
back if they differ. The controller also runs unconditionally every minute so that
non-existence of the target item is reported correctly.

### ReadDesireInformerManagingController

Watches the `ReadDesire` informer and owns the lifecycle of per-`ReadDesire`
`ReadDesireKubernetesController` instances. When a `ReadDesire`'s `spec.targetItem` changes
(GVR, namespace, or name), the old per-instance controller is stopped and a new one is started.
When a `ReadDesire` is deleted, its controller is stopped and discarded.

## Running locally

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [AWS CLI](https://aws.amazon.com/cli/) (for table management)
- A LocalStack Pro auth token in `LOCALSTACK_AUTH_TOKEN` (or use the free image without streams)

### Quick start

```bash
# 1. Start LocalStack (DynamoDB + Streams)
make localstack

# 2. Create Kind cluster + kube-applier-system namespace + RBAC
make kind-setup

# 3. Create the 6 DynamoDB tables for management cluster "mc01"
for suffix in applydesires deletedesires readdesires; do
  for prefix in mc-mc01-specs mc-mc01-status; do
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url=http://127.0.0.1:4566 dynamodb create-table \
      --table-name "${prefix}-${suffix}" \
      --billing-mode PAY_PER_REQUEST \
      --attribute-definitions AttributeName=documentID,AttributeType=S \
      --key-schema AttributeName=documentID,KeyType=HASH \
      --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
  done
done

# 4. Build and run the binary
MANAGEMENT_CLUSTER=mc01 make run-local
```

### desirectl

`desirectl` is a `kubectl`-like CLI for writing and reading desires directly. Build it with:

```bash
make desirectl
```

Configure a context (one-time):

```bash
./desirectl config set-context local \
  --aws-region=us-east-1 \
  --endpoint-url=http://127.0.0.1:4566 \
  --management-cluster=mc01 \
  --cluster-id=mc01

./desirectl config use-context local
```

Apply a manifest (creates an `ApplyDesire` in DynamoDB):

```bash
./desirectl apply -f my-manifest.yaml
```

List resources (reads from the specs table and polls status):

```bash
./desirectl get configmaps
./desirectl get configmap my-configmap -n default
```

Delete a resource (creates a `DeleteDesire` in DynamoDB):

```bash
./desirectl delete configmap my-configmap -n default
```

## Testing

### Unit tests

```bash
make test
```

No external dependencies required.

### Integration tests — database layer (LocalStack only)

Tests raw DynamoDB CRUD and Streams informers against a real LocalStack instance.
No Kind cluster required.

```bash
make localstack
LOCALSTACK_ENDPOINT=http://localhost:4566 \
  go test ./internal/database/... ./internal/database/informers/... \
  -count=1 -v -timeout 120s
```

### Integration tests — controller layer (LocalStack + Kind)

End-to-end tests that run the full `app.Options.Run()` stack — informers, leader election,
and all three controllers — against LocalStack (DynamoDB) and a Kind cluster
(real kube-apiserver). Four scenarios are covered:

| Test | What it verifies |
|---|---|
| `TestIntegration_ApplyDesire` | `ApplyDesire` spec → ConfigMap appears on Kind + `Successful=True` in status |
| `TestIntegration_DeleteDesire` | Pre-created ConfigMap → `DeleteDesire` → ConfigMap gone + `Successful=True` |
| `TestIntegration_ReadDesire` | Pre-created ConfigMap → `ReadDesire` → `status.kubeContent` populated |
| `TestIntegration_OptimisticConcurrency` | Two concurrent `Replace` calls → exactly one `ErrPreconditionFailed` |

```bash
make localstack
make kind-setup

LOCALSTACK_ENDPOINT=http://localhost:4566 \
  KUBECONFIG=$HOME/.kube/config \
  make integration-test
```

Both `LOCALSTACK_ENDPOINT` and `KUBECONFIG` must be set; tests are skipped otherwise.
Each test creates its own uniquely-prefixed tables and removes them in `t.Cleanup`.
