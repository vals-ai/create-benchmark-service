# EKS sandbox provider design

## Goal

Add a Kubernetes sandbox provider that runs benchmark sandboxes in a private Amazon EKS cluster in AWS GovCloud. Tracker and benchmark-service workloads may remain on ECS/Fargate; they reach a private control service in EKS rather than receiving Kubernetes credentials.

The provider must implement the complete shared sandbox contract, including genuine command and download streaming. This branch will add code, tests, and deployment documentation, but it will not create or change cloud resources.

## Boundaries

The first deployment target is EKS, but the framework-facing package remains Kubernetes-specific rather than AWS-specific. Terraform can later supply another conformant cluster and its private network endpoint without changing benchmark requests.

The first implementation will not:

- create an EKS cluster, VPC, load balancer, node group, or DNS record;
- install Kata, Cilium, controllers, or admission policies;
- deploy the control service;
- expose the control service publicly; or
- silently claim support for snapshots whose image and workspace metadata cannot be restored.

The Kubernetes SIG Apps Agent Sandbox project is not the initial runtime. Its current client creates claims from predefined warm pools, while this provider must accept arbitrary OCI images. Its public async command and file APIs also return buffered responses. The design may adopt parts of that project later if those constraints change.

## Topology

```text
Tracker on ECS/Fargate ---------+
                                |  private HTTPS / WebSocket
Benchmark service on ECS -------+----------------------------+
                                                             |
                                               EKS sandbox control service
                                                             |
                                            namespace-scoped Kubernetes RBAC
                                                             |
                                                 Job -> Kata sandbox Pod
```

The Python `KubernetesSandboxProvider` is a client adapter. A small control service inside EKS performs Kubernetes API operations. This keeps cluster credentials and Kubernetes RBAC out of tracker and benchmark-service containers and gives both callers the same lifecycle and streaming behavior.

The control service is stateless. Kubernetes Jobs, Pods, labels, and policies hold runtime state, so multiple control-service replicas can handle requests without session affinity.

## Provider configuration

`KubernetesProviderConfig` joins the public `SandboxProviderConfig` union only after its client and contract tests pass. It contains:

- `type: "kubernetes"`;
- a private control-service URL;
- a bearer token for the first deployment; and
- optional request and connection timeouts.

Cluster name, cloud region, namespace, runtime class, image allowlists, and egress implementation are control-service deployment settings. They do not belong in benchmark requests. This prevents a caller from selecting a weaker runtime or escaping the configured namespace.

The bearer token is expected to come from the existing sandbox-provider secret. A later infrastructure change may replace it with workload identity or mutual TLS without changing `Sandbox` operations.

## Control protocol

The client and service use a versioned private API:

- `POST /v1/sandboxes` creates or reuses a sandbox by requested name and spec fingerprint.
- `GET /v1/sandboxes/{id}` returns current state.
- `GET /v1/sandboxes` filters by labels and paginates with a continuation token.
- `DELETE /v1/sandboxes/{id}` is idempotent.
- `POST /v1/sandboxes/{id}/exec` returns a buffered `ExecResult` for the existing convenience method.
- `WS /v1/sandboxes/{id}/command` streams stdout and stderr chunks followed by one exit event.
- `PUT /v1/sandboxes/{id}/files?path=...` uploads a file.
- `GET /v1/sandboxes/{id}/files?path=...` streams file bytes as they arrive.
- `PUT /v1/sandboxes/{id}/egress` replaces the sandbox allowlist.
- `DELETE /v1/sandboxes/{id}/egress` restores unrestricted egress, matching the existing provider contract.

Every response carries a request ID. Errors use a small stable code set that the client maps to `SandboxNotFoundError`, `SandboxConnectionError`, `SandboxCommandError`, or `SandboxError`; Kubernetes client exceptions never reach framework callers.

The create endpoint treats the requested sandbox name as an idempotency key. If a running sandbox has the same normalized request fingerprint, it is returned. A conflicting specification fails explicitly instead of mutating a running sandbox.

## Kubernetes resources

Each sandbox is a `batch/v1` Job with `backoffLimit: 0`. The Job gives its Pod an owner reference, which is required by EKS VPC CNI network-policy enforcement and gives deletion a clear resource boundary. The sandbox process remains alive until deletion, timeout, or failure. Finished Jobs use `ttlSecondsAfterFinished` as a safety net.

The Job template includes:

- the requested OCI image, pinned or resolved according to the deployment image policy;
- requested CPU, memory, and ephemeral-storage requests and limits;
- requested GPU count as the configured extended resource, with GPU type mapped to a deployment-controlled node label;
- a workspace `emptyDir` bounded by the requested disk size;
- requested environment variables and framework labels;
- the configured Kata `RuntimeClass` and matching node selector;
- a non-mounted service account token and no host namespaces or host paths; and
- a Docker daemon sidecar with a shared socket when nested Docker is enabled by the deployment.

The Docker sidecar is privileged inside the Kata VM boundary, not on the EC2 host. Runtime images and sidecars must be mirrored to GovCloud ECR and pinned by digest before deployment. Images that lack the shell and Docker client required by the existing sandbox contract fail readiness with a useful error.

`ImageSource` is supported first. `ComposeSource` continues to be handled by the existing `ComposeSandbox` wrapper around its outer image. `SnapshotSource` returns an explicit unsupported-source error until a Kubernetes snapshot record can restore both the image reference and workspace volume; a raw CSI `VolumeSnapshot` is insufficient because it does not identify the image.

## Streaming and commands

The control service uses Kubernetes remote exec over the WebSocket streaming protocol. It relays channel data immediately and never accumulates command output for `Sandbox.command`. UTF-8 is decoded incrementally so a multibyte character split across WebSocket frames is preserved. Stderr is merged into the shared output stream to match current provider behavior, and a nonzero exit becomes `SandboxCommandError` after all output is emitted.

`Sandbox.exec` deliberately collects the same command stream because its return type is buffered. A configured output limit prevents an unbounded response from exhausting the control service.

Download uses a binary remote-exec stream and a chunked HTTP response. Neither the control service nor the Python provider joins the chunks. `download_file` collects that stream only because the shared method returns `bytes`; `stream_download` exposes the actual chunks. Upload forwards the request body to remote-exec stdin without adding a second full copy.

Command environment names pass through `validate_command_env`. Commands preserve existing working-directory, timeout, merged-output, and exit-code semantics. Client cancellation closes the upstream stream and triggers best-effort process termination; the command timeout remains the hard backstop.

## Lifecycle and cleanup

Create waits for scheduling, image pull, containers ready, and a successful exec readiness probe within `create_timeout`. Scheduling failures, image-pull failures, quota failures, and runtime errors are reported separately.

The control service records last activity on the Job and enforces `auto_stop_interval` with a janitor loop. A hard maximum lifetime is also configured at deployment. Deletion removes the Job, Pod, egress policy, temporary volumes, and command bookkeeping. Finalizers are used only where cleanup has an external resource; they have a bounded timeout so a broken cleanup path cannot leak Jobs forever.

List operations use Kubernetes label selectors and consume continuation tokens internally until the async generator has yielded `page_size` matches. State is derived from Job and Pod conditions rather than cached in the client.

Transient API, transport, and control-service failures receive bounded retries with jitter. Create reconciliation is safe to retry because of the name and fingerprint rule. Commands and uploads are not automatically replayed after an unknown outcome.

## Network isolation

Sandbox Pods start with denied ingress. Baseline egress remains unrestricted because tracker currently applies allowlists only around commands that request them, and `clear_egress_rules` explicitly restores unrestricted access for every existing provider. A restricted policy permits cluster DNS plus the requested destinations and must fail closed if reconciliation cannot complete. CIDR and domain allowlists use an `EgressPolicyDriver` so the provider contract does not depend on one CNI.

The first production target is Cilium because `CiliumNetworkPolicy` supports DNS-aware `toFQDNs` rules, while standard Kubernetes and EKS VPC CNI policies operate at IP and port layers. Kata plus Cilium has documented networking limitations, so connectivity, MTU, DNS policy, and fail-closed behavior are deployment acceptance tests. If that combination is not reliable in GovCloud, the fallback is a private egress proxy selected by the same driver boundary, not best-effort DNS-to-IP snapshots.

## GovCloud deployment gates

EKS is available in both AWS GovCloud regions, but EKS Fargate is not. Sandbox capacity therefore uses EC2 node groups in private subnets.

Kata requires hardware virtualization. AWS documents Kata on EKS using bare-metal nodes. EC2 added nested virtualization for virtual C8i, M8i, and R8i instances in 2026, but announced it only for commercial regions. The GovCloud rollout must therefore prove that an available bare-metal instance family supports the chosen Kata runtime before any production claim is made. Runtime class remains configurable so this infrastructure choice does not enter benchmark requests.

Additional gates are:

- private-only Kubernetes API access and a private control-service load balancer;
- GovCloud ECR mirrors with digest pins for every image;
- namespace-scoped RBAC and admission rules preventing host access or runtime-class overrides;
- denied baseline ingress plus fail-closed restricted-egress reconciliation;
- multi-AZ control-service replicas and sandbox node capacity;
- encrypted logs and storage with retention configured inside the account;
- Pod and node interruption tests, orphan cleanup tests, and API throttling tests; and
- live proof of lifecycle, true streaming, file transfer, Docker/Compose, egress, retry, timeout, and cleanup behavior.

## Tests and delivery

Implementation is split into reviewable commits and pushed after each verified increment:

1. Architecture and runtime-neutral contract tightening.
2. Public provider configuration and private control-protocol client.
3. Genuine command and download streaming with cancellation and error mapping.
4. Kubernetes control-service resource reconciliation and lifecycle behavior.
5. Egress, cleanup, documentation, and optional live-integration harness.

Unit tests use fake transports and Kubernetes API fixtures. ASGI contract tests run the control service locally. Live tests require an explicitly configured disposable cluster and never run as part of ordinary unit tests. No step in this branch deploys infrastructure.

## References

- [Amazon EKS private cluster requirements](https://docs.aws.amazon.com/eks/latest/userguide/private-clusters.html)
- [Amazon EKS private API endpoint access](https://docs.aws.amazon.com/eks/latest/userguide/config-cluster-endpoint.html)
- [Amazon EKS in AWS GovCloud](https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/govcloud-eks.html)
- [Kubernetes WebSocket streaming](https://kubernetes.io/blog/2024/08/20/websockets-transition/)
- [AWS guidance for Kata Containers on EKS](https://aws.amazon.com/blogs/containers/enhancing-kubernetes-workload-isolation-and-security-using-kata-containers/)
- [EC2 nested virtualization availability](https://aws.amazon.com/about-aws/whats-new/2026/02/amazon-ec2-nested-virtualization-on-virtual/)
- [EKS VPC CNI network-policy behavior](https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy.html)
- [Cilium DNS-aware egress policy](https://docs.cilium.io/en/stable/security/dns/)
- [Cilium with Kata Containers](https://docs.cilium.io/en/stable/network/kubernetes/kata/)
