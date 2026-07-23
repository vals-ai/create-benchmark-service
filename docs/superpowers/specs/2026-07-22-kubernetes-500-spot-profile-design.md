# Kubernetes 500-Spot Profile Design

## Goal

Add and live-test a bounded `scale-500-spot` profile that supports 500 concurrent one-vCPU Kubernetes sandboxes in commercial AWS `vals-dev` without changing the proven 100- or 250-sandbox profiles.

## Profile

The profile uses Spot-only Karpenter capacity, a pinned AL2023 AMI, one NAT gateway, three managed system nodes, two CoreDNS replicas, three control replicas, and a 1,024-connection execution pool. The system-node count lets the control replicas occupy separate nodes and Availability Zones. It sets these workload ceilings:

```text
namespace_pod_quota=600
namespace_cpu_quota=800
namespace_cpu_limit_quota=1800
namespace_memory_quota=800Gi
namespace_memory_limit_quota=3000Gi
namespace_storage_quota=4000Gi
namespace_storage_limit_quota=15000Gi
karpenter_cpu_limit=1024
karpenter_memory_limit=2048Gi
karpenter_root_volume_size=250Gi
required_vcpu_quota=1024
```

The 1,024-vCPU Karpenter ceiling leaves 128 vCPUs below the verified 1,152 Standard Spot vCPU account quota. Namespace quotas include headroom beyond the scale test's 500 CPU, 500 GiB memory, and 2,500 GiB ephemeral-storage requests.

## Live proof

Deploy as `cbs-k8s-spot-500` in account `533328366429`, region `us-east-2`, using only the `vals-dev` profile. Start from zero Karpenter NodeClaims, create all 500 sandboxes as one cold burst, open 500 concurrent command streams, hold them for 360 seconds, release every stream, validate exact final output, and delete every sandbox.

Record total readiness time, p50/p95/p99 readiness, time until all streams are active, node count and mix, Availability Zones, pod restarts, and cleanup results. Destroy the complete Terraform stack after the run and directly verify that no live deployment resources remain.

The in-cluster `ClusterIP` proof completed with all 500 sandboxes ready in 142.7 seconds, readiness p50/p95/p99 of 109.0/134.3/138.3 seconds, and all streams active after 144.8 seconds. All streams held for 360 seconds; the complete proof finished in 512.8 seconds with 16 Spot nodes, 752 vCPUs, about 1.78 TiB of memory, and zero Pod restarts. A local service port-forward was rejected as scale evidence because it selects one control Pod instead of distributing requests across replicas.

## Failure handling

The existing test cleanup runs in `finally`, and the orchestration destroy preserves state until direct AWS checks pass. Terraform foundation cleanup retries transient AWS deletion races up to three times. A failed live proof must still delete its sandboxes; infrastructure remains available for diagnosis until the explicit destroy command runs.

## Scope

This change adds one scale profile, its regression assertions, and matching documentation. It does not alter sandbox resource semantics, introduce a general profile language, change the 2,000-sandbox profiles, commit, push, or deploy outside `vals-dev`.
