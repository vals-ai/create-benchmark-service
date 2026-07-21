# Commercial EKS Sandbox Smoke Design

## Status

Approved for implementation on `jf/test-kubernetes`. This work proves the Kubernetes sandbox provider in a disposable commercial AWS account before adapting the infrastructure to GovCloud.

## Goal

Create a repeatable Terraform workflow that provisions a commercial Amazon EKS test environment, runs the existing Kubernetes sandbox control service inside the cluster, exercises the live provider contract through a local port-forward, and destroys every resource created for the test.

The first pass uses the standard `runc` container runtime. Kata is a separate follow-up gate because it needs runtime-specific node configuration, but it must not change the provider or control-service API.

## Boundaries

This environment is for live provider validation, not production traffic. It does not expose the control service publicly, configure a permanent DNS name, move the tracker into Kubernetes, or provision GovCloud resources.

The first pass validates provider behavior and the deployment boundary. It does not claim the stronger workload isolation required for production until the Kata gate passes.

## Image ownership

The benchmark service continues to choose the main sandbox image through `ImageSource`, or through `ComposeSource.outer` when the shared `ComposeSandbox` wrapper is used. Terraform does not build, rewrite, or choose benchmark images.

The Kubernetes sandbox Pod also contains a pinned Docker daemon sidecar. The main benchmark container uses its Unix socket and remains non-privileged; only the daemon sidecar is privileged. This preserves the existing benchmark-image contract and avoids requiring every benchmark image to start `dockerd` or run privileged.

The AWS workflow builds only the sandbox control-service image. The Docker daemon image is an existing pinned image reference. The commercial smoke may mirror it into the stack's ECR repository, and the GovCloud version must use an approved image in the team's GovCloud ECR registry.

## Architecture

The infrastructure has two Terraform layers so Kubernetes resources can be removed before the EKS API disappears:

```text
infra/kubernetes/
├── workload/       # Cloud-neutral namespace, RBAC, policies, and control service
└── aws/
    ├── foundation/ # Commercial AWS VPC, EKS, nodes, ECR, and IAM
    └── Makefile    # Ordered deploy, test, port-forward, and destroy commands
```

The foundation layer creates a dedicated VPC, public and private subnets, outbound access for private worker nodes, an EKS cluster, one managed node group, ECR storage for the control image, and the IAM roles used by those resources. The EKS API enables private access and restricts public access to an explicitly supplied operator CIDR. No existing VPC, cluster, node group, ECR repository, or IAM role is imported into this disposable stack.

Every AWS and EKS command uses the `vals-dev` profile. Preflight rejects another profile and compares the profile's STS account ID with an explicitly supplied expected account ID before Terraform can plan, apply, test, or destroy.

The workload layer creates the sandbox namespace, service account, namespace-scoped RBAC, `ResourceQuota`, `LimitRange`, `runc` `RuntimeClass`, Cilium installation and policy support, control-service Secret, Deployment, and ClusterIP Service. Cilium runs in AWS VPC CNI chaining mode for the initial test so the current `CiliumNetworkPolicy` egress driver can exercise CIDR and FQDN rules.

The control service uses its in-cluster service account. Sandbox Pods do not mount a service-account token. A local operator reaches the ClusterIP service only through `kubectl port-forward`.

## Workflow

The root Make targets provide one ordered interface:

1. `make kubernetes-aws-plan` validates required tools, AWS identity, region, operator CIDR, Terraform inputs, and both Terraform plans without changing AWS.
2. `make kubernetes-aws-deploy` applies the foundation, builds and pushes the control-service image, records its digest, and applies the workload using pinned image references.
3. `make kubernetes-aws-port-forward` forwards the control service to a local port without creating a load balancer.
4. `make kubernetes-aws-test` runs the opt-in live integration contract against the forwarded endpoint.
5. `make kubernetes-aws-destroy` destroys the workload layer first, destroys the foundation second, and verifies that resources carrying the deployment tags no longer exist.

Deploy and destroy use a unique deployment name and dedicated Terraform state. A failed live test leaves the environment running for inspection; teardown is an explicit command so logs and failed Pods remain available until the operator is finished debugging.

## Live acceptance gates

The commercial smoke must prove:

- authenticated health and private access through port-forwarding;
- create, retry-by-name, get, paginated list, delete, readiness, and automatic cleanup;
- buffered exec and WebSocket command chunks arriving before command completion;
- binary upload, buffered download, and HTTP download chunks arriving before completion;
- client cancellation and command timeout closing the remote exec session;
- benchmark-selected OCI images and a Compose workload using the pinned Docker daemon sidecar;
- CPU, memory, and ephemeral-storage requests and limits;
- temporary CIDR and FQDN allowlists followed by unrestricted egress restoration;
- request ceilings, image allowlist and digest enforcement, error mapping, and janitor cleanup; and
- complete workload-first teardown followed by an AWS tag and Kubernetes namespace residue check.

GPU scheduling is exercised only when the stack is given a GPU node group and test image. Kata isolation is tested in the follow-up runtime configuration. Neither optional gate blocks the initial functional smoke, and neither is reported as passing without the matching infrastructure.

## Failure handling

Preflight failures happen before Terraform apply and identify the missing command, AWS setting, image reference, or CIDR. Terraform apply failures preserve state for retry or destroy. Workload apply cannot run until the foundation outputs a reachable EKS endpoint and ready node group.

The test command reports the failing live gate and keeps the cluster intact. The destroy command is safe to rerun: it attempts the workload layer before the foundation and performs residue checks even when Terraform reports no remaining changes.

## Portability

Cloud-specific networking, identity, registry, and cluster resources stay in `infra/kubernetes/aws/foundation`. The workload layer consumes a Kubernetes connection and image digests, so a later GovCloud, GKE, or AKS foundation can deploy the same control-service protocol without changing benchmark requests.

Kata is introduced as a runtime and node-pool configuration. Kubernetes requires the named `RuntimeClass` handler to exist on the selected nodes; changing from `runc` to Kata therefore remains a deployment choice rather than a request field.

## References

- [Provision an EKS cluster with Terraform](https://developer.hashicorp.com/terraform/tutorials/kubernetes/eks)
- [Manage Kubernetes resources with Terraform](https://developer.hashicorp.com/terraform/tutorials/kubernetes/kubernetes-provider)
- [Amazon EKS identity and access guidance](https://docs.aws.amazon.com/eks/latest/best-practices/identity-and-access-management.html)
- [Cilium with the AWS VPC CNI plugin](https://docs.cilium.io/en/stable/installation/cni-chaining-aws-cni/)
- [Cilium DNS-based policy](https://docs.cilium.io/en/stable/security/policy/layer7/)
- [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/)
