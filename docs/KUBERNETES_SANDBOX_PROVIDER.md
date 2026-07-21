# Kubernetes sandbox provider

## Status

The framework has a registered `kubernetes` provider client and a private control-service protocol. It covers create, get, list, delete, buffered exec, streaming command output, binary upload, buffered and streaming download, and temporary egress restrictions.

This branch does not create or change an EKS cluster, install cluster resources, or deploy the control service. A configured client is runnable only after the private service has been deployed and passed the live gates below.

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

## Security and reliability requirements

- Private load balancer and private DNS only; no public control-service ingress.
- Bearer authentication, request IDs, body and file-size limits, and redacted structured logs.
- Namespace-scoped service account with only the resources and subresources used by the service, including Pod exec.
- Pod Security restricted defaults, non-root control-service container, read-only root filesystem, dropped capabilities, seccomp, and no host namespace or host-path access.
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

Terraform and multi-cloud cluster modules come after this provider boundary is proven. They should supply the same control-service API and keep cloud-specific networking, identity, storage, and node configuration outside benchmark requests.
