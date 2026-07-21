# Kubernetes sandbox provider

## Status

The `benchmark_service.sandbox.kubernetes` package is an experimental scaffold. It defines the boundary between the benchmark-service framework and a Kubernetes sandbox runtime, but it cannot create a sandbox yet.

This first change deliberately does not:

- register Kubernetes in `SandboxProviderConfig`;
- add a Kubernetes SDK or controller dependency;
- install custom resources, Helm charts, or manifests;
- create or change an EKS cluster; or
- deploy anything to AWS GovCloud.

Callers should continue using the existing Daytona or Modal provider configurations until a Kubernetes driver passes the rollout checks below.

## Component boundary

The framework-facing provider stays independent of the Kubernetes runtime:

```text
Benchmark service
  -> KubernetesSandboxProvider
    -> KubernetesRuntimeDriver
      -> KubernetesSandbox
        -> exec, command, upload, download, and egress operations
      -> Kata driver (first implementation)
      -> KubeVirt driver (fallback)
```

`KubernetesSandboxProvider` implements the existing `SandboxProvider` interface and delegates create, get, list, delete, and close operations to a `KubernetesRuntimeDriver`. The driver returns `KubernetesSandbox` objects, which delegate command execution, command streaming, file transfer, download streaming, and egress changes back to the same driver using their sandbox ID.

This keeps Kubernetes API and sandbox-controller calls in one runtime-specific implementation. `KubernetesSandbox` only stores the shared ID, name, and state fields and applies the existing `Sandbox` method signatures.

Kata or KubeVirt selection belongs in deployment configuration, not in a benchmark request. This keeps the existing `SandboxCreateRequest` stable and prevents benchmark definitions from depending on cluster implementation details.

## Portability rules

The first driver and later drivers should follow the same rules:

- Benchmark environments remain OCI images. Runtime-specific machine images are cluster infrastructure, not benchmark inputs.
- Workspace persistence uses a portable storage interface. A replacement sandbox can attach or restore the same workspace without exposing a cloud disk identifier to the benchmark service.
- Sandbox names and labels retain their current retry and lookup meaning.
- File transfer, command streaming, resource requests, and egress rules keep the existing `Sandbox` behavior.
- Provider and driver errors map to the shared sandbox exceptions instead of leaking Kubernetes client errors to callers.

Changing drivers should be straightforward for newly created sandboxes because callers use the shared provider contract. Live migration of a running sandbox between Kata and KubeVirt is outside the first implementation; a runtime change will recreate the sandbox and restore its workspace when persistence is enabled.

## First deployment target

The initial target is a private Amazon EKS cluster in the team's AWS GovCloud account. The EKS API endpoint should be private, worker nodes should run in private subnets, and sandbox, controller, and runtime images should be mirrored into GovCloud ECR. Cluster access, workload identity, logs, storage, and network policy must remain inside the account boundary selected for the service.

Kata is the preferred first driver because it provides a VM boundary while retaining a pod-oriented Kubernetes workflow. KubeVirt remains a driver-level fallback for workloads that require a fuller virtual-machine model or cannot run on the chosen Kata node configuration.

The same provider package can later support other clouds. Terraform should separate cloud-specific cluster modules from the shared Kubernetes platform layer, while each cluster selects a compatible runtime driver through deployment configuration.

## Rollout gates

Kubernetes should not be added to the public provider configuration until all of these gates pass:

1. Implement a Kata-backed `KubernetesRuntimeDriver` against a private, disposable EKS cluster.
2. Mirror every controller, runtime, and benchmark image into GovCloud ECR and pin deployable images by digest.
3. Prove create, get, list, delete, command streaming, file upload/download, Docker and Compose execution, egress restriction, retry-by-name, timeout handling, and cleanup.
4. Verify that failed and interrupted runs do not leave sandboxes, volumes, or network rules behind.
5. Add a Kubernetes provider configuration and register it in `SandboxProviderConfig` only after the live proof passes.
6. Add the KubeVirt driver only if Kata compatibility testing identifies workloads that need it.

Later infrastructure work can add Terraform and deployment packaging once this runtime contract has been proven. That work should be reviewed and deployed separately from this scaffold.
