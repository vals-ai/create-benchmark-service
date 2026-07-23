# Kubernetes Spot Scale Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Spot-only Karpenter path for the existing EKS 2,000-sandbox stress test.

**Architecture:** A new `scale-2000-spot` orchestration profile reuses the large profile's sizing while selecting only Spot capacity. Preflight chooses the matching EC2 quota code, and the selected capacity type becomes an immutable Terraform plan input.

**Tech Stack:** Bash, Terraform, Karpenter, AWS Service Quotas, pytest

## Global Constraints

- Use only the `vals-dev` AWS profile and commercial `us-east-2` default.
- Do not provision or destroy AWS resources during local verification.
- Keep `smoke` and `scale-2000` behavior unchanged.
- Do not create commits or push this work.

---

### Task 1: Specify Spot orchestration behavior

**Files:**
- Modify: `tests/test_kubernetes_aws_script.py`
- Test: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: `KUBERNETES_SCALE_PROFILE`, `KARPENTER_AMI_ALIAS`, and the shell command shim.
- Produces: Assertions for Spot quota rejection, Spot-only Terraform capacity, and unchanged On-Demand quota behavior.

- [x] **Step 1: Extend the AWS shim with the Spot quota**

```bash
*"service-quotas get-service-quota"*"L-34B43A08"*)
  printf '%s\n' "${SHIM_STANDARD_SPOT_VCPU_QUOTA:-5000}"
  ;;
```

- [x] **Step 2: Add a failing Spot plan case**

Run `scale-2000-spot` with a pinned AMI and a 4,999-vCPU Spot quota. Assert that the plan fails before Terraform and names the 5,000 Standard Spot vCPU requirement.

- [x] **Step 3: Run the focused test and confirm the failure**

Run: `uv run pytest tests/test_kubernetes_aws_script.py -q`

Expected: FAIL because `scale-2000-spot` is not accepted yet.

### Task 2: Implement the Spot profile

**Files:**
- Modify: `infra/kubernetes/aws/kubernetes-aws`

**Interfaces:**
- Consumes: `KUBERNETES_SCALE_PROFILE=scale-2000-spot`.
- Produces: `karpenter_capacity_types='["spot"]'`, Spot quota validation, immutable plan metadata, and `TF_VAR_karpenter_capacity_types`.

- [x] **Step 1: Configure explicit capacity types**

Set `smoke` and `scale-2000` to `["on-demand"]`; set `scale-2000-spot` to `["spot"]` while sharing the large profile's existing sizing.

- [x] **Step 2: Select the quota by profile**

Use `L-1216C47A` and `Standard On-Demand` for `scale-2000`; use `L-34B43A08` and `Standard Spot` for `scale-2000-spot`.

- [x] **Step 3: Preserve the capacity choice**

Write and compare `planned_karpenter_capacity_types`, then export `TF_VAR_karpenter_capacity_types` for workload plan and destroy.

- [x] **Step 4: Run the focused test**

Run: `uv run pytest tests/test_kubernetes_aws_script.py -q`

Expected: PASS.

### Task 3: Document and verify the Spot ramp

**Files:**
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`

**Interfaces:**
- Consumes: the existing deploy, scale-test, and destroy commands.
- Produces: Copy/paste commands for Spot quota inspection and the 100, 500, 2,000 target sequence.

- [x] **Step 1: Add the Spot deployment commands**

Document `KUBERNETES_SCALE_PROFILE=scale-2000-spot`, a distinct deployment name, and the Standard Spot quota code.

- [x] **Step 2: Add the progressive test targets**

Document separate 100, 500, and 2,000 invocations and require cleanup between targets.

- [x] **Step 3: Run final verification**

Run:

```bash
uv run pytest tests/test_kubernetes_aws_script.py -q
uv run ruff check tests/test_kubernetes_aws_script.py
uv run basedpyright tests/test_kubernetes_aws_script.py
bash -n infra/kubernetes/aws/kubernetes-aws
terraform -chdir=infra/kubernetes/workload fmt -check
```

Expected: all commands pass without contacting or changing AWS.
