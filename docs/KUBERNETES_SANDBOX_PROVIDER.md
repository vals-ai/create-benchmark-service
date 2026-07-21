# Kubernetes sandbox provider

The Kubernetes provider sends the shared sandbox API to a private control service in a
cluster. Benchmark services do not receive Kubernetes credentials or cluster settings.

This repository includes a disposable commercial AWS smoke for that deployment shape. It
creates EKS and its VPC, installs the control service and Cilium, runs the live provider
contract, and destroys the resources. The smoke is for development only: it uses `runc`,
one `m6i.xlarge` node, one NAT gateway, and `us-east-2` by default. It does not test Kata,
GPUs, GovCloud, multi-cloud support, or production readiness.

## How the provider runs

```text
Tracker or benchmark service
  -> private control-service HTTP and WebSocket API
EKS control service
  -> namespace-scoped Kubernetes API
Sandbox Job
  -> benchmark-provided primary image
  -> privileged Docker daemon sidecar when Docker is enabled
```

The client supports idempotent create, get, list, delete, buffered and streaming commands,
binary upload, buffered and streaming download, command timeout, and temporary egress
allowlists. The control service creates Jobs and per-sandbox network policies in the
`benchmark-sandboxes` namespace.

For a hosted caller, the request-scoped provider secret contains only the private service
connection:

```json
{
  "type": "kubernetes",
  "KUBERNETES_API_URL": "https://sandbox-control.internal",
  "KUBERNETES_API_TOKEN": "...",
  "KUBERNETES_CONNECT_TIMEOUT": 10,
  "KUBERNETES_REQUEST_TIMEOUT": 60
}
```

Do not put an EKS endpoint, kubeconfig, namespace, runtime class, AWS region, or node details
in this secret.

## Commercial AWS smoke

### Prerequisites

**AWS account.** Use the `vals-dev` profile. The wrapper rejects every other profile, checks
the expected 12-digit account ID, and requires an identity in the commercial `arn:aws:`
partition. Set the expected account ID yourself; do not derive it from the current session,
because the value is a guard against using the wrong account.

The identity needs permission to create and delete the EKS, IAM, VPC, EC2, EBS, and ECR
resources in the Terraform plan. It also needs STS identity lookup, EKS kubeconfig access,
ECR push access, and Resource Groups Tagging API reads used by the cleanup check.

**Operator network.** Supply an IPv4 CIDR no broader than `/24`. Use the public address of
the machine running the smoke as a `/32` whenever possible. Terraform enables the EKS
private endpoint and limits its public endpoint to this CIDR. The control service itself is
a Kubernetes `ClusterIP`; the smoke does not create public control-service ingress.

**Tools.** Install `aws`, `curl`, `docker`, `git`, `kubectl`, `openssl`, `terraform`, and
`uv`. The foundation module accepts Terraform 1.10+, but the complete lifecycle requires
Terraform 1.11+ because the workload keeps the generated API token out of saved state and
plan data. Docker must be running.

**Images.** Choose a benchmark test image that the EKS nodes can pull and pin it with a
`sha256` digest. The deploy does not build or mirror this image. A private image registry
must already allow pulls from the node role.

**Charges.** Deploy creates chargeable AWS resources, including an EKS cluster, one
on-demand `m6i.xlarge` node, and one NAT gateway. EBS storage, ECR storage, network traffic,
and public IPv4 use can also incur charges. Billing can continue after a failed deploy, so
run destroy and check its result even when deploy or test fails.

### Image roles

The three image settings serve different purposes:

1. The Terraform-backed deploy builds the control-service image from this repository,
   pushes it to the smoke ECR repository, and deploys it by digest.
2. `TEST_KUBERNETES_IMAGE` is the digest-pinned image supplied by the benchmark. It becomes
   the primary sandbox container. The deploy does not build it.
3. The deploy pulls `docker:28.3.3-dind`, copies it to the smoke ECR repository without
   rebuilding it, resolves the ECR digest, and uses it as the privileged Docker daemon
   sidecar.

`TEST_KUBERNETES_COMPOSE_IMAGE` is optional. It is the outer sandbox image for the Compose
contract and must contain the Docker CLI and Compose plugin. The mirrored
`docker:28.3.3-dind` image can fill this role while it talks to the separate daemon sidecar.
The Compose `main` service still uses `TEST_KUBERNETES_IMAGE`; the outer image does not
replace the benchmark image.

### Configure the shell

Replace the account ID, operator address, image repository, and image digest before running
these commands:

```bash
export AWS_PROFILE=vals-dev
export AWS_ACCOUNT_ID=123456789012
export AWS_OPERATOR_CIDR=203.0.113.10/32
export AWS_REGION=us-east-2
export KUBERNETES_DEPLOYMENT_NAME=cbs-kubernetes-smoke
export TEST_KUBERNETES_IMAGE=registry.example.com/benchmark/image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Confirm that the profile shows the account you set and an `arn:aws:` ARN:

```bash
aws sts get-caller-identity --profile vals-dev
```

Keep the same exported values for plan, deploy, test, and destroy. A different deployment
name points at a different local state and runtime directory.

### Plan

```bash
make kubernetes-aws-plan
```

Plan initializes and validates both Terraform roots and saves the foundation plan under
`infra/kubernetes/aws/.state/$KUBERNETES_DEPLOYMENT_NAME/`. It does not create the workload
plan yet because that provider needs the live EKS API endpoint. Review the saved foundation
plan before deploy.

### Deploy

```bash
make kubernetes-aws-deploy
```

Deploy applies the reviewed foundation plan, builds and pushes the control image, mirrors
the Docker daemon image, writes a private kubeconfig and generated API token, and applies
the workload. It waits for Cilium and the control-service Deployment to become ready.

The foundation includes a two-AZ VPC, private worker subnets, one NAT gateway, EKS 1.35,
one on-demand `m6i.xlarge` node with a 100 GiB encrypted `gp3` root volume, and an immutable
ECR repository. The workload installs Cilium in AWS VPC CNI chaining mode, creates the
`benchmark-sandboxes` namespace, and configures the `runc` RuntimeClass, quota, limits,
namespace-scoped RBAC, token Secret, control Deployment, and `ClusterIP` Service.

`runc` plus a privileged DinD sidecar is not the planned isolation model for untrusted
workloads. Keep this cluster disposable and use it only for the smoke contract.

### Test

The test command starts its own local port-forward, waits for `/health`, and runs the opt-in
live contract:

```bash
make kubernetes-aws-test
```

It checks lifecycle idempotency, real-time command output, cancellation, timeout handling,
large binary streaming, and temporary egress rules. It always attempts to delete the
sandboxes it creates and reports cleanup failures. It skips the Compose contract unless
`TEST_KUBERNETES_COMPOSE_IMAGE` is set.

To use the mirrored Docker image for the optional Compose outer, load the runtime metadata
after deploy and export its digest:

```bash
source "infra/kubernetes/aws/.runtime/${KUBERNETES_DEPLOYMENT_NAME}/deployment.env"
export TEST_KUBERNETES_COMPOSE_IMAGE="$docker_image"
make kubernetes-aws-test
```

The runtime file contains the generated control-service token. It is mode `0600`, ignored
by Git, and must not be printed, copied into logs, or committed.

For manual API work, run this blocking command in a separate terminal:

```bash
make kubernetes-aws-port-forward
```

It opens only a local tunnel at `http://127.0.0.1:8080`; it does not deploy a load balancer
or make the control service public.

### Destroy

Tests and failed deploys do not destroy the stack automatically. Always run:

```bash
make kubernetes-aws-destroy
```

For a full deployment, destroy first removes the Kubernetes workload and checks that the
`benchmark-sandboxes` namespace is gone. It then destroys EKS, ECR, the node group, NAT
gateway, and VPC, and queries AWS for resources with the smoke's project and deployment
tags. Only after the required namespace check and the tagged-resource check are empty does
it delete the local state, kubeconfig, token, and runtime metadata.

Successful Terraform destroy plus an empty tagged-resource query is the cleanup signal.
Check the AWS console if billing or service quotas show anything unexpected; some AWS
resources can take time to disappear from service views.

## Recovery and retries

The deploy writes foundation recovery metadata before the first apply and keeps separate
Terraform state for the foundation and workload. If deploy stops after resources start
creating, keep the same environment values and run:

```bash
make kubernetes-aws-destroy
```

Do not delete `infra/kubernetes/aws/.state/` or `.runtime/` by hand. Those files contain the
state, phase, kubeconfig, and token needed to clean up a partial deployment. After destroy
succeeds, run plan again before another deploy so the saved plan matches empty state.

Destroy is retryable. If workload cleanup stops, the next call retries that phase. Once the
namespace is confirmed absent, the runtime metadata moves to the foundation phase so a
later retry does not call a deleted Kubernetes API. A failed tagged-resource check also
keeps local metadata for another destroy attempt.

A failed deploy can still leave chargeable resources. If local state or runtime metadata is
lost before cleanup, stop and reconcile the tagged AWS resources before creating another
deployment with the same name.

## Local checks

The provider unit tests do not need a cluster:

```bash
uv run pytest \
  tests/test_kubernetes_client.py \
  tests/test_kubernetes_control_app.py \
  tests/test_kubernetes_resources.py \
  tests/test_kubernetes_backend.py \
  -q
```

The live contract skips unless its control URL, token, and benchmark image variables are
set:

```bash
TEST_KUBERNETES_CONTROL_URL=https://sandbox-control.internal \
TEST_KUBERNETES_CONTROL_TOKEN=... \
TEST_KUBERNETES_IMAGE=registry.internal/benchmark@sha256:... \
uv run pytest tests/integration/test_kubernetes_control_service.py -q
```

## Later deployment targets

The next isolation target is Kata Containers on compatible nodes in AWS GovCloud, with all
workload images, storage, secrets, logs, and data paths kept in the team's cloud account and
VPC. That design still needs GovCloud instance and runtime validation; this `runc` smoke is
not evidence that Kata works there.

Future Terraform roots can add GovCloud and other cloud environments behind the same
private control-service API. Cloud-specific VPC, identity, storage, registry, and node
configuration stays out of benchmark requests, so benchmark code and provider secrets do
not change when the cluster implementation changes.
