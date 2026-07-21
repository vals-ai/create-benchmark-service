# Kubernetes sandbox provider

## Status

The framework has a registered `kubernetes` provider client and a private control-service protocol. It covers create, get, list, delete, buffered exec, streaming command output, binary upload, buffered and streaming download, and temporary egress restrictions.

This branch does not create or change an EKS cluster, install cluster resources, or deploy the control service. A configured client is runnable only after the private service has been deployed and passed the live gates below.

The `kubernetes-sandbox-control` entrypoint starts the service against an already prepared cluster. Importing the package or starting the client never installs Kubernetes resources outside individual sandbox Jobs and their per-sandbox policies.

## Configuration

The request-scoped provider secret contains only the private service connection:

```json
{
  "type": "kubernetes",
  "KUBERNETES_API_URL": "https://sandbox-control.internal",
  "KUBERNETES_API_TOKEN": "...",
  "KUBERNETES_CONNECT_TIMEOUT": 10,
  "KUBERNETES_REQUEST_TIMEOUT": 60
}
```

The URL must be reachable from the tracker or benchmark service. Put the token in the same secret flow used for other provider credentials. Do not place the EKS endpoint, kubeconfig, namespace, runtime class, cloud region, or node details in this request.

## Boundary

EKS is the sandbox provider. The tracker and benchmark services do not need to move to Kubernetes and do not receive Kubernetes credentials.

```text
Tracker or benchmark service
  -> HTTPS and WebSocket
Private sandbox control service in EKS
  -> namespace-scoped Kubernetes API
Job -> Kata-isolated sandbox Pod
  -> workspace volume and optional DinD sidecar
```

The framework client translates the shared `SandboxProvider` operations to the private API. The control service handles Kubernetes resources, readiness, cleanup, command execution, file transfer, and network policy. This boundary keeps provider selection portable: another cluster implementation can expose the same API without changing benchmark code.

## Operation contract

- Lifecycle reads and idempotent writes use bounded retries. Commands and uploads are not replayed after an unknown outcome.
- `command()` uses WebSocket events and yields stdout and stderr chunks as they arrive. A nonzero terminal event becomes `SandboxCommandError` after prior output has been delivered.
- `stream_download()` yields response bytes without joining them. `download_file()` intentionally buffers those chunks for the existing shared return type.
- Baseline egress is unrestricted, matching the existing providers. `modify_egress_rules()` installs a temporary allowlist; `clear_egress_rules()` restores unrestricted egress.
- Control-service errors use stable codes plus a request ID. Kubernetes client exceptions do not cross the service boundary.

## Workload rules

- Direct creates accept `ImageSource`. Images must match the deployment allowlist and be resolved to an approved digest before a Pod starts.
- Compose uses the existing `ComposeSandbox` wrapper with a Docker-in-Docker outer image. The DinD sidecar stays inside the sandbox VM boundary.
- `SnapshotSource` is rejected until image and workspace restore semantics are defined and tested.
- CPU, memory, ephemeral disk, GPU count, and GPU type map to Pod requests, limits, and scheduling constraints. Supported GPU types are a deployment allowlist.
- Sandbox identity comes from the server. User names and labels are stored only under the control service's label prefix and cannot set arbitrary Kubernetes metadata.

## Initial EKS target

The first deployment target is Amazon EKS in AWS GovCloud with a private API endpoint and private worker subnets. EKS-managed control plane use is acceptable; all workload images, logs, storage, secrets, and data paths remain in the team's account and VPC boundary.

Kata Containers is the first isolation target. In GovCloud, plan for compatible bare-metal worker nodes unless AWS documents nested virtualization support for the selected GovCloud instance type. EKS Fargate is not part of this design. Runtime selection belongs to the cluster deployment, not the provider request.

Cilium is the first egress-policy backend because the required policy includes domain-aware rules. The control service calls an internal egress-driver interface so another implementation can replace it without changing the public provider.

## Control-service environment

Required:

- `KUBERNETES_SANDBOX_API_TOKEN`
- `KUBERNETES_SANDBOX_DOCKER_IMAGE`

Common deployment settings:

- `KUBERNETES_SANDBOX_NAMESPACE` (default `benchmark-sandboxes`)
- `KUBERNETES_SANDBOX_RUNTIME_CLASS` (default `kata-qemu`)
- `KUBERNETES_SANDBOX_ALLOWED_IMAGE_PREFIXES` (comma-separated)
- `KUBERNETES_SANDBOX_REQUIRE_IMAGE_DIGEST` (set `true` in shared environments)
- `KUBERNETES_SANDBOX_HARD_LIFETIME_SECONDS` and `KUBERNETES_SANDBOX_FINISHED_TTL_SECONDS`
- `KUBERNETES_SANDBOX_JANITOR_INTERVAL_SECONDS`
- `KUBERNETES_SANDBOX_EXEC_OUTPUT_LIMIT_BYTES`
- `KUBERNETES_SANDBOX_UPLOAD_LIMIT_BYTES`
- `KUBERNETES_SANDBOX_MAX_CREATE_TIMEOUT_SECONDS`
- `KUBERNETES_SANDBOX_MAX_VCPU`, `KUBERNETES_SANDBOX_MAX_MEMORY_GIB`, `KUBERNETES_SANDBOX_MAX_DISK_GIB`, and `KUBERNETES_SANDBOX_MAX_GPU`
- `KUBERNETES_SANDBOX_GPU_RESOURCE_NAME` and `KUBERNETES_SANDBOX_GPU_TYPE_LABEL`
- `KUBERNETES_SANDBOX_HOST` and `KUBERNETES_SANDBOX_PORT`

`KUBERNETES_SANDBOX_ALLOW_LOCAL_KUBECONFIG` defaults to `false`. Enable it only for local development against a disposable cluster. The normal process loads in-cluster service-account credentials.

The service account is namespace-scoped. Its Role needs `get`, `list`, `create`, `patch`, and `delete` for `batch/jobs`; `get` and `list` for Pods; `get` and `create` for `pods/exec`; `get`, `create`, `update`, and `delete` for `networking.k8s.io/networkpolicies`; and `get`, `create`, `update`, and `delete` for `cilium.io/ciliumnetworkpolicies`. It does not need Secrets, Nodes, cluster-wide workloads, or RBAC mutation.

Apply a namespace `ResourceQuota` and `LimitRange` for aggregate capacity, plus connection and request concurrency limits at the private ingress. The service enforces per-sandbox CPU, memory, disk, GPU, create-time, command-output, and upload ceilings; Kubernetes admission remains the final aggregate-capacity guard.

Expose `/health` and the `/v1/sandboxes` API only through a private load balancer and private DNS. Store the API token in the deployment secret mechanism, mount it only into the control service, and rotate it independently of Kubernetes credentials.

## Security and reliability requirements

- Private load balancer and private DNS only; no public control-service ingress.
- Bearer authentication, request IDs, body and file-size limits, and redacted structured logs.
- Namespace-scoped service account with only the resources and subresources used by the service, including Pod exec.
- Restricted settings for the control-service Pod and the main sandbox container, with dropped capabilities, seccomp, and no host namespace or host-path access. DinD is the one privileged container and must be admitted only in the Kata sandbox namespace; a namespace-wide restricted Pod Security profile would reject it.
- Kata runtime class enforced on every sandbox Pod. Sandbox containers do not receive service-account tokens or Kubernetes API access.
- Default-deny ingress for sandboxes. Baseline egress remains unrestricted until a temporary allowlist is installed.
- Image registry allowlist, digest resolution, pull policy, resource ceilings, quota, and per-caller concurrency limits.
- Retry-by-name, create timeout, automatic stop deadline, finalizers, and a janitor for expired Jobs, Pods, volumes, and network policies.
- Command cancellation closes the remote exec session. Client disconnects must not leak control-service tasks.

## Live rollout gates

Before selecting this provider in a real run:

1. Deploy the control service and policies to a disposable private EKS cluster using a separate infrastructure change.
2. Mirror and pin control, runtime, DinD, and benchmark images in GovCloud ECR.
3. Prove create, retry-by-name, get, paginated list, delete, readiness, timeout, and automatic cleanup.
4. Prove command chunks and large file chunks arrive before completion, and cancellation stops the remote process.
5. Prove binary upload/download, Compose/DinD, CPU and memory limits, ephemeral storage, and every supported GPU mapping.
6. Prove temporary domain/CIDR allowlists and unrestricted restore with DNS and IPv4/IPv6 behavior documented.
7. Interrupt the client and control service during create, command, upload, download, and delete; verify there are no orphan resources.
8. Run the shared sandbox integration contract against the private endpoint and record the image digests, cluster version, runtime version, and test results.

Local checks do not need a cluster:

```bash
uv run pytest tests/test_kubernetes_client.py tests/test_kubernetes_control_app.py tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py -q
```

After a private deployment exists, run the opt-in live contract:

```bash
TEST_KUBERNETES_CONTROL_URL=https://sandbox-control.internal \
TEST_KUBERNETES_CONTROL_TOKEN=... \
TEST_KUBERNETES_IMAGE=registry.internal/benchmark@sha256:... \
uv run pytest tests/integration/test_kubernetes_control_service.py -q
```

Terraform and multi-cloud cluster modules come after this provider boundary is proven. They should supply the same control-service API and keep cloud-specific networking, identity, storage, and node configuration outside benchmark requests.
