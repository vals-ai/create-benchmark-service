# Kubernetes 250-Spot Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded Spot profile for 250 concurrent Kubernetes sandboxes and benchmark its cold-start and streaming performance in `vals-dev`.

**Architecture:** Keep the proven 100-sandbox profile unchanged and add `scale-250-spot` with quotas derived from the existing one-vCPU sandbox plus Docker defaults and 20 percent headroom. Reuse the existing EKS, Karpenter, control service, live scale test, and complete Terraform destroy path.

**Tech Stack:** Bash, Terraform, EKS, Karpenter, pytest, AWS CLI

## Global Constraints

- Use only commercial AWS account `533328366429` through the `vals-dev` profile in `us-east-2`.
- Never use the production `vals` profile.
- Keep `smoke`, `scale-100-spot`, `scale-2000`, and `scale-2000-spot` behavior unchanged.
- Use Spot only and require at least 512 Standard Spot vCPUs.
- Hold 250 active command streams for 360 seconds and clean up every sandbox even on failure.
- Destroy all test infrastructure and verify absence after the benchmark.
- Do not commit or push.

---

### Task 1: Specify the 250 profile

**Files:**
- Modify: `tests/test_kubernetes_aws_script.py`
- Test: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: `KUBERNETES_SCALE_PROFILE=scale-250-spot` and a pinned `KARPENTER_AMI_ALIAS`.
- Produces: Observable orchestration assertions for the profile name, Spot quota, capacity type, and bounded namespace/Karpenter limits.

- [x] **Step 1: Add the failing profile expectations**

Require the orchestration script to expose these 250-profile values:

```text
namespace_pod_quota=300
namespace_cpu_quota=400
namespace_cpu_limit_quota=900
namespace_memory_quota=400Gi
namespace_memory_limit_quota=1500Gi
namespace_storage_quota=2000Gi
namespace_storage_limit_quota=7500Gi
karpenter_cpu_limit=512
karpenter_memory_limit=1024Gi
karpenter_root_volume_size=250Gi
required_vcpu_quota=512
```

- [x] **Step 2: Run the focused test and confirm it fails**

Run: `UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run pytest tests/test_kubernetes_aws_script.py -q`

Expected: FAIL because `scale-250-spot` is not accepted or configured.

### Task 2: Implement and document the profile

**Files:**
- Modify: `infra/kubernetes/aws/kubernetes-aws`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`

**Interfaces:**
- Consumes: the existing scale-profile selection and Terraform variable wiring.
- Produces: a Spot-only `scale-250-spot` plan/deploy/destroy path and copy/paste commands.

- [x] **Step 1: Add the minimal profile case**

Keep two managed system nodes, two CoreDNS replicas, three control replicas, a 1,024-connection pool, and 250-GiB node volumes. Apply the Task 1 limits and require quota code `L-34B43A08` at 512 vCPUs.

- [x] **Step 2: Update accepted-profile validation and documentation**

Document the new profile beside `scale-100-spot`, including `cbs-k8s-spot-250`, a 250 target, and an initial creation batch of 250 for a directly comparable cold burst.

- [x] **Step 3: Run focused verification**

Run:

```bash
UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run pytest tests/test_kubernetes_aws_script.py -q
UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run ruff check tests/test_kubernetes_aws_script.py
bash -n infra/kubernetes/aws/kubernetes-aws
terraform fmt -check -recursive infra/kubernetes/aws
```

Expected: all commands pass.

### Task 3: Benchmark and destroy

**Files:**
- Reuse: `tests/integration/test_kubernetes_scale.py`
- Reuse: `infra/kubernetes/aws/kubernetes-aws`

**Interfaces:**
- Consumes: a private control-service URL, generated control token, pinned benchmark image, and `TEST_KUBERNETES_SCALE_TARGET=250`.
- Produces: cold-start total/p50/p95/p99, 250-stream readiness and hold proof, Pod restart count, Spot node mix, cleanup proof, and direct AWS absence checks.

- [x] **Step 1: Plan and deploy in `vals-dev`**

Use account `533328366429`, region `us-east-2`, deployment `cbs-k8s-spot-250`, profile `scale-250-spot`, and a pinned AL2023 Karpenter AMI alias.

- [x] **Step 2: Run the cold 250 burst**

Set target, creation batch, and cleanup batch to 250; hold all command streams for 360 seconds. Record readiness latency, active streams, Pod restarts, node count, instance types, and Availability Zones.

- [x] **Step 3: Verify scale-down and destroy**

Confirm zero sandbox Jobs and Pods, observe Karpenter consolidation, run Terraform destroy, and directly verify that the EKS cluster, VPC, endpoints, instances, ECR repository, Spot service role, runtime files, and state files are absent.

- [x] **Step 4: Run final local checks**

Run the non-live test suite, Ruff, basedpyright, shell syntax, Terraform formatting and validation, Helm lint, and `git diff --check`.
