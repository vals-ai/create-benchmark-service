# Kubernetes sandbox provider

The Kubernetes provider keeps cluster credentials and deployment choices out of benchmark
requests. A benchmark service receives only the URL and token for a private control service;
the control service reconciles sandbox Jobs inside its namespace.

This repository currently contains a commercial AWS EKS implementation for `vals-dev`.
The contract path and Spot profiles through 500 concurrent sandboxes have been live-tested.
The larger profile prepares the quotas, network, request tier, and node autoscaling for a
2,000-sandbox test; that capacity is not considered proven until its own burst and stream
soak succeeds through the private VPC path used by benchmark services.

## Runtime path

```text
Tracker or benchmark service
  -> private control-service HTTP API (HTTPS when a private ingress terminates TLS)
     -> Kubernetes API for Job lifecycle and shared Job/Pod watches
     -> private Pod IP:8787 for command and file data
Sandbox Job
  -> benchmark-provided primary image
  -> injected static sandbox agent as the primary process
  -> privileged Docker daemon sidecar when nested Docker is enabled
```

The package is split by the boundary it serves:

- `client.py`, `config.py`, `provider.py`, and `sandbox.py` are the benchmark-service
  provider surface.
- `control/app.py` exposes the private control-service HTTP and WebSocket contract.
- `control/kubernetes.py` handles sandbox lifecycle, while `control/data_plane.py` handles
  commands and file transfer.
- `control/api.py` and `control/cache.py` contain Kubernetes API access and shared watches.
- `control/resources.py` builds sandbox Jobs, and `control/egress.py` manages network
  policy.
- `infra/kubernetes/aws/` provisions and destroys EKS infrastructure;
  `infra/kubernetes/workload/` installs the cluster workload.

Provider-facing commands use `POST /v1/sandboxes/{id}/command` with newline-delimited JSON.
Blank lines are heartbeats, so a quiet command does not look dead to an idle proxy. The
client never replays a command after it has been sent because the outcome would be unknown.
Closing the response stream cancels the process group inside the sandbox.

The control image contains a small static agent. An init container copies that binary into
an `emptyDir`, and the benchmark container runs it from the shared volume. Benchmark images
do not need Python or a preinstalled SDK. Commands still require `sh`, matching the earlier
Kubernetes exec implementation. The agent streams stdout and stderr, enforces command
timeouts, and transfers file bodies without base64 expansion. It also emits blank NDJSON
heartbeats. The control-to-agent hop has a three-heartbeat read deadline, so a lost node
becomes a terminal stream error while an intentionally quiet command stays connected.

Each agent accepts a bearer token derived for that sandbox only. A shared `NetworkPolicy`
allows ingress to the agent port only from control-service Pods. Sandbox-to-sandbox ingress
is denied, and a per-sandbox Cilium policy controls egress destinations. Cilium WireGuard
encryption is enabled for sandbox Pod traffic that crosses nodes; same-node traffic remains
inside that node. The AWS CNI chaining MTU setting is enabled with it, and the shared node
security group permits Cilium's UDP `51871` tunnel only between cluster nodes. This network
configuration still needs live validation before it is treated as a GovCloud control.

The Terraform workload does not grant `pods/exec` to the control service. The older
Kubernetes exec implementation and WebSocket control route remain in the package for an
independently prepared compatibility deployment, but they are not the normal Terraform
path.

## Control-plane scaling

The request replicas share Kubernetes state through one Job watch and one Pod watch per
process. Sandbox readiness and Pod-IP lookup use that cache instead of polling once per
request. A synthetic unit test has 2,000 readiness waiters share those two watches.

Activity annotations are written at most once per configured interval by each control
replica handling a busy sandbox. Cleanup runs as a single `CronJob`, so adding request
replicas does not multiply janitor work. The control Deployment has topology spread, a
disruption budget, bounded HTTP pools, and a configurable direct-agent connection pool.

Every command hop requires a terminal `exit` or `error` event. An early EOF or malformed
event becomes a connection failure instead of looking like a successful command. Live
commands and uploads are not replayed automatically because the first attempt may still
have changed the sandbox.

The provider supports idempotent create, get, list, delete, buffered exec, streaming
commands, binary upload, buffered and streaming download, command timeout, and temporary
egress allowlists.

For a hosted caller, the request-scoped secret contains only the private connection:

```json
{
  "type": "kubernetes",
  "KUBERNETES_API_URL": "https://sandbox-control.internal",
  "KUBERNETES_API_TOKEN": "...",
  "KUBERNETES_CONNECT_TIMEOUT": 10,
  "KUBERNETES_REQUEST_TIMEOUT": 60,
  "KUBERNETES_STREAM_READ_TIMEOUT": 45,
  "KUBERNETES_MAX_CONNECTIONS": 256,
  "KUBERNETES_MAX_KEEPALIVE_CONNECTIONS": 64
}
```

Do not put an EKS endpoint, kubeconfig, namespace, runtime class, AWS region, node details,
or the in-Pod agent token in this secret.

The control service sends an outer blank heartbeat every 15 seconds. The default stream read
timeout allows three missed heartbeats before the provider reports a connection error, so a
stalled VPC or proxy path cannot leave a benchmark command waiting forever.

## Commercial AWS profiles

All profiles use EKS 1.35, Karpenter 1.12.1, three Availability Zones, `/19` private
subnets, VPC CNI prefix delegation, and private ECR, S3, CloudWatch Logs, and STS endpoints.
The managed node group runs system components. Karpenter supplies sandbox nodes from current
`c`, `m`, and `r` families with 4 through 64 vCPUs.

| Setting | `smoke` | `scale-100-spot` | `scale-250-spot` | `scale-500-spot` | `scale-2000` | `scale-2000-spot` |
|---|---:|---:|---:|---:|---:|---:|
| Sandbox capacity | On-Demand | Spot only | Spot only | Spot only | On-Demand | Spot only |
| NAT gateways | 1 | 1 | 1 | 1 | 3, one per AZ | 3, one per AZ |
| System nodes | 1 | 2 | 2 | 3 | 3 | 3 |
| CoreDNS replicas | 2 | 2 | 2 | 2 | 6 | 6 |
| Control replicas | 3 | 3 | 3 | 3 | 6 | 6 |
| Active-command activity write | 30 seconds | 30 seconds | 30 seconds | 30 seconds | 300 seconds | 300 seconds |
| Namespace Pod quota | 20 | 150 | 300 | 600 | 2,200 | 2,200 |
| CPU request quota | 8 | 160 | 400 | 800 | 5,000 | 5,000 |
| CPU limit quota | 80 | 350 | 900 | 1,800 | 8,500 | 8,500 |
| Memory request quota | 16 GiB | 160 GiB | 400 GiB | 800 GiB | 9,000 GiB | 9,000 GiB |
| Memory limit quota | 160 GiB | 600 GiB | 1,500 GiB | 3,000 GiB | 17,000 GiB | 17,000 GiB |
| Ephemeral-storage request quota | 100 GiB | 700 GiB | 2,000 GiB | 4,000 GiB | 44,000 GiB | 44,000 GiB |
| Ephemeral-storage limit quota | 800 GiB | 3,000 GiB | 7,500 GiB | 15,000 GiB | 82,000 GiB | 82,000 GiB |
| Karpenter CPU ceiling | 64 | 256 | 512 | 1,024 | 5,000 | 5,000 |
| Karpenter memory ceiling | 128 GiB | 512 GiB | 1,024 GiB | 2,048 GiB | 10,000 GiB | 10,000 GiB |
| Karpenter node root volume | 100 GiB | 250 GiB | 250 GiB | 250 GiB | 500 GiB | 500 GiB |

Request and limit quotas are separate because the Docker sidecar receives namespace
defaults in addition to the benchmark container's resources. Treat the larger profile as a
high-cost test environment. Karpenter capacity is cold: nodes are created only when pending
sandboxes need them, so creation should be ramped in waves rather than sent as one 2,000-Job
write burst. New Karpenter nodes start with Cilium's `agent-not-ready` taint; Cilium removes
it only after the node data path is ready, so sandbox Pods cannot outrun policy and
encryption setup during a cold burst. Live sandbox Pods also carry Karpenter's
`do-not-disrupt` annotation, preventing voluntary consolidation or drift from discarding an
active command or `emptyDir` workspace. Empty nodes can still consolidate, and forceful EC2
interruptions remain possible.

Sandbox nodes have a permanent `NoSchedule` taint that only sandbox Jobs tolerate. Control,
DNS, and other shared workloads therefore stay on the managed system group. The 500 profile
pins three system nodes so its three control replicas can occupy separate nodes and
Availability Zones. The 2,000 profiles also start three system nodes and raise CoreDNS to
six replicas. The EKS CoreDNS add-on supplies its disruption budget and Availability Zone
spread settings.

The scale profile uses a larger node root volume so ephemeral-storage requests do not leave
most of a large instance's CPU and memory stranded. The volume is still deleted with the
node, and Karpenter creates nodes only in response to pending sandbox Pods.

An open command refreshes its Job activity annotation even when it produces no output, so
the idle janitor cannot remove a healthy long-running stream. The scale profile coalesces
those writes to one per command every five minutes, keeping 2,000 quiet streams near seven
Kubernetes API patches per second instead of roughly sixty-seven.

The smoke profile uses `al2023@latest` by default for convenience. The scale profile refuses
to plan unless `KARPENTER_AMI_ALIAS` pins an `al2023@vYYYYMMDD` release. That prevents a long
or expensive run from changing node images between plan, deploy, and recovery.

The selected profile is written to the private runtime metadata and reloaded during
destroy. That keeps the workload and foundation destroy inputs identical to the provisioned
inputs.

## Prerequisites

Use the `vals-dev` AWS profile. The wrapper rejects every other profile, checks the expected
12-digit account ID, and requires an identity in the commercial `arn:aws:` partition. Set
the expected account ID yourself; do not derive it from the current session, because the
value guards against using the wrong account.

The identity needs permission to create and delete EKS, IAM, VPC, EC2, EBS, ECR, SQS, and
VPC endpoint resources. It also needs STS identity lookup, EKS kubeconfig access, ECR push
access, Resource Groups Tagging API reads used by the cleanup check, and
`servicequotas:GetServiceQuota` for the scale-plan guard.

Supply an operator IPv4 CIDR no broader than `/24`; use the current public address as a
`/32` where possible. EKS has a private endpoint and a public endpoint limited to this CIDR.
The control service is a `ClusterIP` and is not exposed publicly.

Install `aws`, `curl`, `docker`, `git`, `helm`, `kubectl`, `openssl`, `terraform`, and `uv`.
Terraform 1.11 or newer is required because the workload API token is ephemeral and is kept
out of saved plans and state. Docker must be running.

Choose a benchmark image and pin it with a `sha256` digest. Direct Kubernetes pulls need
node access to that registry. The smoke does not configure third-party registry credentials
or mirror the benchmark image.

The optional Compose path runs the benchmark image as the Compose `main` service through a
Docker daemon sidecar. The outer image must contain the Docker CLI and Compose plugin. The
Compose service image must be anonymously pullable unless registry authentication is added.

## Image roles

The deployment uses three images:

1. The control image is built from this repository and pushed by digest. It runs the control
   service and contains the static sandbox-agent binary copied into every sandbox.
2. `TEST_KUBERNETES_IMAGE` is the benchmark-provided primary image. The deploy does not
   build it.
3. `docker:28.3.3-dind` is mirrored into the deployment ECR repository and pinned by digest.
   It is the Docker daemon sidecar, not a replacement for the benchmark image.

`TEST_KUBERNETES_COMPOSE_IMAGE` is optional. It is the outer sandbox image for the Compose
contract. The mirrored Docker image can fill this role while it talks to the separate daemon
sidecar.

## Configure and plan

Replace the account, operator address, and benchmark image before running these commands:

```bash
export AWS_PROFILE=vals-dev
export AWS_ACCOUNT_ID=123456789012
export AWS_OPERATOR_CIDR=203.0.113.10/32
export AWS_REGION=us-east-2
export KUBERNETES_DEPLOYMENT_NAME=cbs-kubernetes-smoke
export KUBERNETES_SCALE_PROFILE=smoke
export TEST_KUBERNETES_IMAGE=registry.example.com/benchmark/image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Confirm the account and commercial partition:

```bash
aws sts get-caller-identity --profile vals-dev
```

Keep the same exported values for plan and deploy. Plan writes a mode-`0600` input record,
and deploy refuses to apply if the account, profile, region, operator CIDR, scale profile,
AMI alias, or deployment name changed. Destroy reloads the saved region, operator CIDR,
and scale profile, while the account ID, profile, and deployment name remain required
guardrails.

```bash
make kubernetes-aws-plan
```

Plan initializes and validates both Terraform roots and saves the foundation plan under
`infra/kubernetes/aws/.state/$KUBERNETES_DEPLOYMENT_NAME/`. The workload plan is created
after foundation apply because its providers need the live EKS endpoint.

## Deploy and contract test

Deployment is intentionally a separate command:

```bash
make kubernetes-aws-deploy
```

It applies the reviewed foundation plan, builds and pushes the control image, mirrors the
Docker image, writes a private kubeconfig and generated control token, then applies the
workload. It waits for Cilium and the control Deployment to become ready and requires the
Cilium agent to report WireGuard encryption enabled.

The system node group uses `runc`. Sandbox capacity is also `runc` in this first commercial
AWS test. A privileged Docker sidecar is not suitable isolation for hostile workloads; keep
the cluster disposable until Kata or another verified isolation runtime replaces it.

The ordinary live contract starts a local port-forward and exercises one sandbox:

```bash
make kubernetes-aws-test
```

It covers lifecycle idempotency, incremental HTTP output, cancellation, timeout exit code
124, binary file streaming, Compose, and temporary egress rules. It always tries to delete
the sandboxes it creates. A failed test does not destroy the cluster.

The generated runtime file contains the control token and image digests. It is mode `0600`,
ignored by Git, and must not be printed or committed.

For manual work, run this in a separate terminal:

```bash
make kubernetes-aws-port-forward
```

The tunnel listens only on `http://127.0.0.1:8080`. A service port-forward selects one
control Pod and sends traffic through one Kubernetes API tunnel; it does not exercise
ClusterIP load balancing across the control replicas. Use it for the contract test only,
never as evidence for a concurrent scale target.

## Scale live gates

Provision the large profile only after reviewing AWS service quotas and the Terraform plan:

```bash
aws service-quotas get-service-quota \
  --profile vals-dev \
  --region "$AWS_REGION" \
  --service-code ebs \
  --quota-code L-7A658B76 \
  --query Quota.Value
release_version="$(aws ssm get-parameter \
  --profile vals-dev \
  --region "$AWS_REGION" \
  --name /aws/service/eks/optimized-ami/1.35/amazon-linux-2023/x86_64/standard/recommended/release_version \
  --query Parameter.Value \
  --output text)"
```

Choose one capacity path. For the final On-Demand qualification:

```bash
aws service-quotas get-service-quota \
  --profile vals-dev \
  --region "$AWS_REGION" \
  --service-code ec2 \
  --quota-code L-1216C47A \
  --query Quota.Value
export KUBERNETES_SCALE_PROFILE=scale-2000
export KUBERNETES_DEPLOYMENT_NAME=cbs-k8s-scale-2000
export KARPENTER_AMI_ALIAS="al2023@v${release_version##*-}"
make kubernetes-aws-plan
make kubernetes-aws-deploy
```

For a cheaper 100-sandbox Spot test:

```bash
export KUBERNETES_SCALE_PROFILE=scale-100-spot
export KUBERNETES_DEPLOYMENT_NAME=cbs-k8s-spot-100
export KARPENTER_AMI_ALIAS="al2023@v${release_version##*-}"
make kubernetes-aws-plan
make kubernetes-aws-deploy
```

For a cold 250-sandbox Spot test with additional namespace and Karpenter headroom:

```bash
export KUBERNETES_SCALE_PROFILE=scale-250-spot
export KUBERNETES_DEPLOYMENT_NAME=cbs-k8s-spot-250
export KARPENTER_AMI_ALIAS="al2023@v${release_version##*-}"
make kubernetes-aws-plan
make kubernetes-aws-deploy
```

For a cold 500-sandbox Spot test beneath a 1,152-vCPU account quota:

```bash
export KUBERNETES_SCALE_PROFILE=scale-500-spot
export KUBERNETES_DEPLOYMENT_NAME=cbs-k8s-spot-500
export KARPENTER_AMI_ALIAS="al2023@v${release_version##*-}"
make kubernetes-aws-plan
make kubernetes-aws-deploy
```

For the full 2,000-sandbox Spot ramp:

```bash
aws service-quotas get-service-quota \
  --profile vals-dev \
  --region "$AWS_REGION" \
  --service-code ec2 \
  --quota-code L-34B43A08 \
  --query Quota.Value
export KUBERNETES_SCALE_PROFILE=scale-2000-spot
export KUBERNETES_DEPLOYMENT_NAME=cbs-k8s-spot-2000
export KARPENTER_AMI_ALIAS="al2023@v${release_version##*-}"
make kubernetes-aws-plan
make kubernetes-aws-deploy
```

The plan requires 256 Standard Spot vCPUs for `scale-100-spot`, 512 for
`scale-250-spot`, 1,024 for `scale-500-spot`, or 5,000 vCPUs from the selected capacity quota
for either 2,000-sandbox profile. Spot profiles never fall back to On-Demand, so an
unavailable Spot pool remains visible as a test failure. Also compare existing gp3 use plus
the planned node volumes with the EBS quota; `GetServiceQuota` reports the limit, not current
use.

Run the opt-in soak from a host or runner that reaches the control `ClusterIP` through the
same private VPC path used by benchmark services:

```bash
export TEST_KUBERNETES_CONTROL_URL=http://kubernetes-sandbox-control.benchmark-sandboxes.svc.cluster.local:8080
export TEST_KUBERNETES_CONTROL_TOKEN=...
export TEST_KUBERNETES_IMAGE=registry.internal/benchmark@sha256:...
export TEST_KUBERNETES_SCALE_CREATE_BATCH=100
export TEST_KUBERNETES_SCALE_HOLD_SECONDS=360
export TEST_KUBERNETES_SCALE_TARGET=100
uv run pytest tests/integration/test_kubernetes_scale.py -q -s
export TEST_KUBERNETES_SCALE_CREATE_BATCH=250
export TEST_KUBERNETES_SCALE_TARGET=250
uv run pytest tests/integration/test_kubernetes_scale.py -q -s
export TEST_KUBERNETES_SCALE_CREATE_BATCH=500
export TEST_KUBERNETES_SCALE_CLEANUP_BATCH=250
export TEST_KUBERNETES_SCALE_TARGET=500
uv run pytest tests/integration/test_kubernetes_scale.py -q -s
export TEST_KUBERNETES_SCALE_TARGET=2000
uv run pytest tests/integration/test_kubernetes_scale.py -q -s
```

Use the private HTTPS hostname instead when the runner is outside the cluster and a private
TLS ingress has been added.

The 500-sandbox qualification ran from an in-cluster runner against the `ClusterIP`, starting
with zero Karpenter NodeClaims. All 500 one-vCPU sandboxes became ready, opened command
streams, held them for six minutes, released cleanly, and were deleted. The three control
replicas handled 160, 174, and 166 create requests, confirming that the service path spread
the burst across replicas.

| Target | Cold total | p50 | p95 | p99 | All streams | Full proof | Spot nodes | Pod restarts |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 119.5s | 104.0s | 113.5s | 117.5s | 120.6s | 481.8s | 7 | 0 |
| 250 | 126.5s | 102.6s | 118.5s | 122.7s | 128.4s | 495.2s | 11 | 0 |
| 500 | 142.7s | 109.0s | 134.3s | 138.3s | 144.8s | 512.8s | 16 | 0 |

At 500, the Spot nodes supplied 752 vCPUs and about 1.78 TiB of memory. These measurements
describe one commercial AWS `vals-dev` qualification and are not an availability guarantee.

Advance to the next target only after the previous run reports that every sandbox was
deleted. The test reports batch progress, cold-start p50/p95/p99, time to all active streams,
and total duration. A background consumer actively reads every stream during the hold, which
exercises heartbeats, detects a disconnect before release, and crosses the scale profile's
five-minute activity-refresh boundary. It then uses a second command to release the streams,
checks every terminal event, and deletes all created Jobs. Passing it once is not enough for
production sizing; also record Karpenter/node failures, Spot interruptions, API throttling,
disconnects, and cleanup results during repeated runs.

## Destroy

Always destroy a smoke or scale deployment when testing is finished:

```bash
make kubernetes-aws-destroy
```

Destroy removes the Kubernetes workload first and confirms the namespace is gone. It then
destroys Karpenter resources, EKS, ECR, the system node group, VPC endpoints, NAT gateways,
and the VPC. Finally it queries AWS for live resources carrying the deployment tags. Local
state, kubeconfig, token, and runtime metadata are removed only after Terraform succeeds and
no live tagged resource remains.

Destroy retries the Terraform foundation phase up to three times to cover AWS deletion races,
such as Spot requests closing just after the service-linked role deletion starts. The command
is also safe to run again: if workload cleanup stops, the next call retries that phase. Once
the namespace is confirmed absent, the saved phase moves to foundation cleanup so a later
retry does not call a deleted Kubernetes API.

Do not delete `infra/kubernetes/aws/.state/` or `.runtime/` by hand. They hold the state and
recovery metadata needed to clean up a partial deployment. If those files are lost, stop and
reconcile tagged AWS resources before creating another deployment with the same name.

## Local verification

No cluster is needed for the provider, cache, control-service, and agent tests:

```bash
uv run pytest \
  tests/test_kubernetes_agent.py \
  tests/test_kubernetes_client.py \
  tests/test_kubernetes_control_app.py \
  tests/test_kubernetes_cache.py \
  tests/test_kubernetes_resources.py \
  tests/test_kubernetes_backend.py \
  tests/test_kubernetes_aws_script.py \
  -q

(cd infra/kubernetes/agent && go test -race ./...)
terraform -chdir=infra/kubernetes/aws/foundation validate
terraform -chdir=infra/kubernetes/workload validate
helm lint infra/kubernetes/charts/karpenter-sandbox \
  --set clusterName=test-cluster \
  --set nodeRole=test-node-role
```

The ordinary live contract skips unless its private control URL, token, and benchmark image
are set:

```bash
TEST_KUBERNETES_CONTROL_URL=https://sandbox-control.internal \
TEST_KUBERNETES_CONTROL_TOKEN=... \
TEST_KUBERNETES_IMAGE=registry.internal/benchmark@sha256:... \
uv run pytest tests/integration/test_kubernetes_control_service.py -q
```

## Later deployment targets

The next isolation target is Kata Containers on compatible nodes in AWS GovCloud, with
images, storage, secrets, logs, and data paths kept in the team's account and VPC. Kata and
Firecracker remain separate experiments; this `runc` deployment is not evidence that either
runtime works with the benchmark images or nested Docker contract.

Future Terraform roots can add GovCloud and other clouds behind the same private control API.
Cloud-specific VPC, identity, registry, storage, and node settings stay out of benchmark
requests, so the provider secret and benchmark code do not change when the cluster
implementation changes.
