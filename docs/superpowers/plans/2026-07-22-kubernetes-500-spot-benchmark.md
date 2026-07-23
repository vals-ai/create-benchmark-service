# Kubernetes 500-Spot Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded Spot profile for 500 concurrent Kubernetes sandboxes and benchmark its cold-start and streaming performance in `vals-dev`.

**Architecture:** Extend the existing explicit scale-profile selection with `scale-500-spot`, using doubled 250-profile namespace quotas and a 1,024-vCPU Karpenter ceiling beneath the verified 1,152-vCPU account quota. Reuse the existing EKS, Karpenter, control service, scale test, cleanup, and Terraform destroy paths.

**Tech Stack:** Bash, Terraform, EKS, Karpenter, pytest, AWS CLI

## Global Constraints

- Use only commercial AWS account `533328366429` through `vals-dev` in `us-east-2`.
- Never use the production `vals` profile.
- Keep all existing scale-profile behavior unchanged.
- Use Spot only, pin `KARPENTER_AMI_ALIAS=al2023@v20260714`, and require 1,024 Standard Spot vCPUs.
- Create 500 one-vCPU sandboxes in one cold batch and hold 500 streams for 360 seconds.
- Clean up every sandbox, destroy all test infrastructure, and verify direct AWS absence.
- Do not commit or push.

---

### Task 1: Specify the 500 profile

**Files:**
- Modify: `tests/test_kubernetes_aws_script.py`
- Test: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: `KUBERNETES_SCALE_PROFILE=scale-500-spot`, pinned `KARPENTER_AMI_ALIAS`, and the Standard Spot quota response.
- Produces: exact orchestration assertions for the 500-profile limits and quota preflight.

- [x] **Step 1: Add failing expectations**

Extend the existing orchestration test with a 1,023-vCPU rejection and a valid plan/deploy/destroy flow requiring the exact values in the design spec.

- [x] **Step 2: Verify red**

Run: `UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run pytest tests/test_kubernetes_aws_script.py -q`

Expected: fail because `scale-500-spot` is not accepted or configured.

### Task 2: Implement and document the profile

**Files:**
- Modify: `infra/kubernetes/aws/kubernetes-aws`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`

**Interfaces:**
- Consumes: the existing explicit profile switch and Terraform environment wiring.
- Produces: a Spot-only `scale-500-spot` plan/deploy/destroy path and copy/paste commands.

- [x] **Step 1: Add the minimal profile case**

Set pod quota 600; CPU request/limit quotas 800/1,800; memory request/limit quotas 800/3,000 GiB; storage request/limit quotas 4,000/15,000 GiB; Karpenter limits 1,024 CPU and 2,048 GiB; root volume 250 GiB; and required Spot quota 1,024.

- [x] **Step 2: Document the profile**

Add `scale-500-spot` beside the 250 profile with deployment name `cbs-k8s-spot-500`, target and create batch 500, cleanup batch 250, and the same six-minute hold.

- [x] **Step 3: Verify green**

Run the focused pytest, Ruff on the changed test, shell syntax, Terraform formatting, and `git diff --check`; all must pass.

### Task 3: Benchmark and destroy

**Files:**
- Reuse: `tests/integration/test_kubernetes_scale.py`
- Reuse: `infra/kubernetes/aws/kubernetes-aws`

**Interfaces:**
- Consumes: the private control URL, generated token, pinned benchmark image, and a target/create batch of 500.
- Produces: readiness percentiles, 500-stream hold proof, node and pod observations, cleanup proof, and direct AWS absence checks.

- [x] **Step 1: Plan and deploy in `vals-dev`**

Use deployment `cbs-k8s-spot-500`, profile `scale-500-spot`, pinned AMI `al2023@v20260714`, and the pinned Python benchmark image digest.

- [x] **Step 2: Run the cold proof**

Confirm zero NodeClaims, create 500 sandboxes in one batch, hold all 500 streams for 360 seconds, and record timings, node mix, Availability Zones, failed pods, and restarts.

- [x] **Step 3: Clean up and destroy**

Confirm zero remaining scale-test sandboxes, run the complete destroy path, and directly check EKS, EC2, ECR, IAM, Spot requests, runtime files, and Terraform state.

- [x] **Step 4: Run final local verification**

Run the non-live Python suite, Go tests, Ruff, basedpyright, shell syntax, Terraform formatting and validation, Helm lint, and `git diff --check`.
