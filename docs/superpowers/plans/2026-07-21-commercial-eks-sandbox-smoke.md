# Commercial EKS Sandbox Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable commercial AWS EKS environment that runs the Kubernetes sandbox control service, proves the live provider contract through a local port-forward, and completely tears down its Kubernetes and AWS resources.

**Architecture:** A foundation Terraform root creates a dedicated VPC, EKS cluster, managed node group, IAM, and ECR repository. A separate workload Terraform root connects through a supplied kubeconfig path/context and creates Cilium, the namespace-scoped control service, RBAC, quotas, and the `runc` runtime class; the AWS orchestration generates that kubeconfig with `vals-dev` and applies and destroys the two roots in dependency order.

**Tech Stack:** Terraform 1.10+, AWS provider 6.x, terraform-aws-vpc 6.6.1, terraform-aws-eks 21.24.0, Kubernetes provider 3.2.1, Helm provider 3.x, Amazon EKS 1.35, Amazon Linux 2023 managed nodes, Cilium 1.19.6, Docker, AWS CLI, kubectl, Bash, Python 3.12, pytest.

## Global Constraints

- Provision only the commercial `aws` partition in the first smoke; reject GovCloud and China partitions in preflight.
- Require `AWS_PROFILE=vals-dev`; reject every other profile and pass `vals-dev` explicitly to AWS CLI, Terraform, and EKS token commands.
- Require `AWS_ACCOUNT_ID` and compare it with `aws sts get-caller-identity` before plan, apply, test, or destroy.
- Default to `us-east-2`, EKS 1.35, one `m6i.xlarge` on-demand managed node, and one NAT gateway; expose all as Terraform variables.
- Enable private EKS API access and limit public EKS API access to the required `AWS_OPERATOR_CIDR` value.
- Keep the control service as `ClusterIP`; use `kubectl port-forward` for the smoke.
- Keep `infra/kubernetes/workload` cloud-neutral: it accepts a kubeconfig path/context and contains no AWS or EKS authentication commands.
- Build only the control-service image. The benchmark request supplies the main sandbox image; mirror the pinned Docker daemon image into the stack ECR repository without rebuilding it.
- Keep the benchmark container non-privileged. Only the Docker daemon sidecar is privileged.
- Use `runc` for this smoke. Do not claim Kata isolation until a separate runtime/node configuration passes its live gate.
- Tag every AWS resource with `Project=create-benchmark-service-kubernetes`, `Deployment=${deployment_name}`, and `ManagedBy=Terraform`.
- Use separate local state files for foundation and workload. Destroy workload first, verify its namespace is gone, then destroy foundation and verify no tagged AWS resources remain.
- A failed live test leaves the environment running for inspection. Destruction is always explicit.
- Preserve the unrelated untracked `tests/__init__.py`; never stage it.
- Commit each implementation task separately, pull before every push, and never force push.

## File Structure

- `.dockerignore`: exclude local, test, and Git files from the control-service build context.
- `.gitignore`: exclude deployment runtime files and local Terraform state while retaining provider lock files.
- `infra/kubernetes/Dockerfile.control`: reproducible non-root control-service image.
- `infra/kubernetes/aws/foundation/versions.tf`: Terraform, provider, and module version constraints.
- `infra/kubernetes/aws/foundation/variables.tf`: commercial AWS, network, cluster, node, and tag inputs.
- `infra/kubernetes/aws/foundation/main.tf`: VPC, EKS, managed node group, add-ons, and ECR repository.
- `infra/kubernetes/aws/foundation/outputs.tf`: values consumed by image publishing and the workload layer.
- `infra/kubernetes/workload/versions.tf`: Kubernetes and Helm providers using a supplied kubeconfig path/context.
- `infra/kubernetes/workload/variables.tf`: kubeconfig, image, token, limits, and Cilium inputs.
- `infra/kubernetes/workload/main.tf`: Cilium and cloud-neutral Kubernetes resources.
- `infra/kubernetes/workload/outputs.tf`: namespace and service names used by port-forward and verification.
- `infra/kubernetes/aws/kubernetes-aws`: checked orchestration for plan, deploy, port-forward, test, and destroy.
- `infra/kubernetes/aws/Makefile`: short operator-facing commands around the orchestration script.
- `tests/test_kubernetes_aws_script.py`: focused tests for preflight, ordering, and failure preservation.
- `tests/integration/test_kubernetes_control_service.py`: expanded live provider gates.
- `Makefile`: root aliases for commercial EKS operations.
- `docs/KUBERNETES_SANDBOX_PROVIDER.md`: commercial smoke setup, cost warning, test, debugging, and teardown.

---

### Task 1: Package the control service as a non-root container

**Files:**
- Create: `.dockerignore`
- Create: `infra/kubernetes/Dockerfile.control`

**Interfaces:**
- Consumes: the existing `kubernetes-sandbox-control` project entrypoint and repository `uv.lock`.
- Produces: an OCI image whose default command is `kubernetes-sandbox-control` and whose process runs as UID/GID 10001.

- [ ] **Step 1: Confirm the image does not exist**

Run:

```bash
test ! -f infra/kubernetes/Dockerfile.control
```

Expected: exit 0.

- [ ] **Step 2: Add the build context exclusions**

Create `.dockerignore` with:

```dockerignore
.git
.github
.pytest_cache
.ruff_cache
.venv
docs
tests
**/__pycache__
*.pyc
*.tfstate
*.tfstate.*
infra/kubernetes/aws/.runtime
infra/kubernetes/aws/.state
```

- [ ] **Step 3: Add the pinned multi-stage control image**

Create `infra/kubernetes/Dockerfile.control` with:

```dockerfile
FROM ghcr.io/astral-sh/uv:0.7.13 AS uv
FROM python:3.12-slim-bookworm

COPY --from=uv /uv /uvx /bin/

RUN groupadd --gid 10001 sandbox-control \
    && useradd --uid 10001 --gid 10001 --create-home sandbox-control

WORKDIR /app
COPY pyproject.toml uv.lock README.md .python-version Makefile ./
COPY src ./src
COPY cli ./cli
COPY templates ./templates
RUN mkdir .github
ARG PACKAGE_VERSION=0.0.0
RUN SETUPTOOLS_SCM_PRETEND_VERSION="${PACKAGE_VERSION}" uv sync --frozen --no-dev --no-editable

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 10001:10001
EXPOSE 8080
CMD ["kubernetes-sandbox-control"]
```

- [ ] **Step 4: Build and inspect the image locally**

Run:

```bash
docker build \
  -f infra/kubernetes/Dockerfile.control \
  --build-arg PACKAGE_VERSION="0.0.0+$(git rev-parse --short=12 HEAD)" \
  -t kubernetes-sandbox-control:test \
  .
docker image inspect kubernetes-sandbox-control:test --format '{{.Config.User}} {{json .Config.Cmd}}'
docker run --rm --entrypoint python kubernetes-sandbox-control:test -c 'import benchmark_service.sandbox.kubernetes.control.main'
```

Expected: the inspection prints `10001:10001 ["kubernetes-sandbox-control"]`; the import exits 0.

- [ ] **Step 5: Commit and push**

```bash
git add .dockerignore infra/kubernetes/Dockerfile.control
git commit -m "Package Kubernetes sandbox control service"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 2: Add the disposable commercial AWS foundation

**Files:**
- Create: `infra/kubernetes/aws/foundation/versions.tf`
- Create: `infra/kubernetes/aws/foundation/variables.tf`
- Create: `infra/kubernetes/aws/foundation/main.tf`
- Create: `infra/kubernetes/aws/foundation/outputs.tf`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `aws_profile`, `aws_region`, `deployment_name`, `operator_cidr`, `kubernetes_version`, `node_instance_types`, and common tags.
- Produces: `cluster_name`, `cluster_endpoint`, `cluster_certificate_authority_data`, `image_repository_url`, `aws_region`, and `deployment_tags`.

- [ ] **Step 1: Establish the validation failure**

Run:

```bash
terraform -chdir=infra/kubernetes/aws/foundation init -backend=false
```

Expected: failure because the Terraform root does not exist.

- [ ] **Step 2: Add provider and module constraints**

Create `versions.tf` with Terraform `>= 1.10.0, < 2.0.0`, `hashicorp/aws ~> 6.0`, and `hashicorp/random ~> 3.7`. Pin module sources in `main.tf` to `terraform-aws-modules/vpc/aws` version `6.6.1` and `terraform-aws-modules/eks/aws` version `21.24.0`.

- [ ] **Step 3: Add validated inputs and shared tags**

Define these defaults and validations in `variables.tf`:

```hcl
variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  type    = string
  default = "vals-dev"
  validation {
    condition     = var.aws_profile == "vals-dev"
    error_message = "aws_profile must be vals-dev for this disposable smoke."
  }
}

variable "deployment_name" {
  type    = string
  default = "cbs-kubernetes-smoke"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,31}$", var.deployment_name))
    error_message = "deployment_name must be a 3-32 character lowercase DNS label."
  }
}

variable "operator_cidr" {
  type = string
  validation {
    condition     = can(cidrhost(var.operator_cidr, 0)) && tonumber(split("/", var.operator_cidr)[1]) >= 24
    error_message = "operator_cidr must be an IPv4 CIDR no broader than /24."
  }
}

variable "kubernetes_version" {
  type    = string
  default = "1.35"
}

variable "node_instance_types" {
  type    = list(string)
  default = ["m6i.xlarge"]
}
```

Configure the AWS provider with `profile = var.aws_profile` and `region = var.aws_region`. Create locals for the cluster name and the three mandatory tags. Do not accept caller overrides for those mandatory tag keys.

- [ ] **Step 4: Build the disposable VPC, EKS cluster, and ECR repository**

In `main.tf`:

- select two available AZs;
- create `10.42.0.0/16` with two `/20` private subnets and two `/24` public subnets;
- enable one NAT gateway, DNS hostnames, and EKS subnet tags;
- create EKS 1.35 with private endpoint access, public endpoint access restricted to `operator_cidr`, cluster-creator admin access, and `coredns`, `kube-proxy`, and `vpc-cni` add-ons;
- create one AL2023 x86_64 managed node group with desired/minimum 1, maximum 2, 100 GiB disk, and the `node.cilium.io/agent-not-ready=true:NoExecute` taint;
- disable EKS deletion protection for this disposable environment; and
- create one immutable, scan-on-push ECR repository with `force_delete = true` for the control and mirrored daemon images.

Use the module output names from EKS 21.24.0 exactly; do not copy an older module example.

- [ ] **Step 5: Export only the workload inputs**

Add outputs for:

```hcl
output "cluster_name" { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_certificate_authority_data" {
  value     = module.eks.cluster_certificate_authority_data
  sensitive = true
}
output "image_repository_url" { value = aws_ecr_repository.sandbox.repository_url }
output "aws_region" { value = var.aws_region }
output "deployment_tags" { value = local.tags }
```

- [ ] **Step 6: Ignore only local runtime and state data**

Append:

```gitignore
# Disposable Kubernetes smoke state and secrets
infra/kubernetes/aws/.runtime/
infra/kubernetes/aws/.state/
infra/kubernetes/aws/**/.terraform/
```

Do not ignore `.terraform.lock.hcl`.

- [ ] **Step 7: Format and validate the foundation**

Run:

```bash
terraform -chdir=infra/kubernetes/aws/foundation init -backend=false
terraform -chdir=infra/kubernetes/aws/foundation fmt -check -recursive
terraform -chdir=infra/kubernetes/aws/foundation validate
```

Expected: initialization and validation succeed without contacting or modifying AWS.

- [ ] **Step 8: Commit and push**

```bash
git add .gitignore infra/kubernetes/aws/foundation
git commit -m "Add disposable EKS foundation"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 3: Add the cloud-neutral Kubernetes workload layer

**Files:**
- Create: `infra/kubernetes/workload/versions.tf`
- Create: `infra/kubernetes/workload/variables.tf`
- Create: `infra/kubernetes/workload/main.tf`
- Create: `infra/kubernetes/workload/outputs.tf`

**Interfaces:**
- Consumes: `kubeconfig_path`, `kubeconfig_context`, `control_image`, `docker_image`, `allowed_image_prefixes`, and `api_token`.
- Produces: `namespace`, `service_name`, and `runtime_class_name`.

- [ ] **Step 1: Establish the validation failure**

Run:

```bash
terraform -chdir=infra/kubernetes/workload init -backend=false
```

Expected: failure because the workload Terraform root does not exist.

- [ ] **Step 2: Configure kubeconfig-based authentication**

Require Kubernetes provider `3.2.1` and Helm provider `~> 3.0`. Require non-empty absolute `kubeconfig_path` and non-empty `kubeconfig_context` values. Configure both providers from those values:

```hcl
provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kubeconfig_context
}

provider "helm" {
  kubernetes = {
    config_path    = var.kubeconfig_path
    config_context = var.kubeconfig_context
  }
}
```

Do not reference AWS, EKS, cloud region, or cloud credentials in this root. Require non-empty digest references for `control_image` and `docker_image`, require at least one non-empty `allowed_image_prefixes` entry, and mark `api_token` sensitive with a minimum length of 32.

- [ ] **Step 3: Install Cilium in AWS VPC CNI chaining mode**

Create a pinned `helm_release` for Cilium 1.19.6 in `kube-system` with:

```hcl
values = [yamlencode({
  cni = {
    chainingMode = "aws-cni"
    exclusive    = false
  }
  enableIPv4Masquerade = false
  routingMode          = "native"
  operator = {
    replicas = 1
  }
})]
```

Enable `wait`, `atomic`, a 15-minute timeout, and cleanup on failure. The live egress test, not successful Helm installation alone, decides whether DNS policy works in this mode.

- [ ] **Step 4: Create namespace safeguards and runtime configuration**

Create:

- namespace `benchmark-sandboxes` with `pod-security.kubernetes.io/enforce=privileged`, and restricted audit/warn labels because the Docker daemon sidecar requires privilege;
- `RuntimeClass` named `runc` with handler `runc`;
- `ResourceQuota` for 20 Pods, 8 requested/limited CPU, 16 GiB requested/limited memory, and 100 GiB requested/limited ephemeral storage; and
- `LimitRange` defaults of 250m/256Mi requests and 2 CPU/4Gi limits so the daemon sidecar always receives resources.

- [ ] **Step 5: Create least-privilege control-service RBAC**

Create a service account with no AWS IAM role. Its namespace Role must allow only:

```text
batch/jobs: get,list,create,patch,delete
core/pods: get,list
core/pods/exec: get,create
networking.k8s.io/networkpolicies: get,create,update,delete
cilium.io/ciliumnetworkpolicies: get,create,update,delete
```

Bind only that Role to the control-service account. Do not grant Secrets, Nodes, cluster-wide workload, or RBAC mutation access.

- [ ] **Step 6: Deploy the private control service**

Create one Secret containing `KUBERNETES_SANDBOX_API_TOKEN`. Create a one-replica Deployment that:

- runs the pinned `control_image` as UID/GID 10001;
- uses RuntimeDefault seccomp, drops all capabilities, forbids privilege escalation, and uses a read-only root filesystem with writable empty directories for `/tmp` and the Python cache;
- loads the token from the Secret;
- sets namespace `benchmark-sandboxes`, runtime `runc`, the pinned `docker_image`, the comma-joined `allowed_image_prefixes`, digest enforcement, 2 vCPU, 4 GiB memory, 20 GiB disk, zero GPU, 10-minute create timeout, 60-second janitor interval, and port 8080;
- exposes unauthenticated `/health` readiness and liveness probes; and
- uses rolling update with `maxUnavailable = 0` and `maxSurge = 1`.

Create a ClusterIP Service named `kubernetes-sandbox-control` on port 8080. Do not create Ingress, LoadBalancer, NodePort, or public DNS resources.

- [ ] **Step 7: Export stable operator names**

Output exactly:

```hcl
output "namespace" { value = kubernetes_namespace_v1.sandboxes.metadata[0].name }
output "service_name" { value = kubernetes_service_v1.control.metadata[0].name }
output "runtime_class_name" { value = kubernetes_runtime_class_v1.runc.metadata[0].name }
```

- [ ] **Step 8: Format and validate the workload configuration**

Run:

```bash
terraform -chdir=infra/kubernetes/workload init -backend=false
terraform -chdir=infra/kubernetes/workload fmt -check -recursive
terraform -chdir=infra/kubernetes/workload validate
```

Expected: both provider configurations and all resources validate without applying them.

- [ ] **Step 9: Commit and push**

```bash
git add infra/kubernetes/workload
git commit -m "Add Kubernetes sandbox workload stack"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 4: Add checked deploy and destroy orchestration

**Files:**
- Create: `infra/kubernetes/aws/kubernetes-aws`
- Create: `infra/kubernetes/aws/Makefile`
- Create: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: command `plan|deploy|port-forward|test|destroy` and environment values `AWS_ACCOUNT_ID`, `AWS_PROFILE=vals-dev`, `AWS_REGION`, `AWS_OPERATOR_CIDR`, `KUBERNETES_DEPLOYMENT_NAME`, `TEST_KUBERNETES_IMAGE`, and optional `DIND_SOURCE_IMAGE`.
- Produces: two local state files, a mode-0600 runtime environment file, pinned ECR digests, and ordered Terraform operations.

- [ ] **Step 1: Write the failing orchestration test**

Add one table-driven pytest test with local executable shims. It must prove:

- missing `AWS_ACCOUNT_ID` and `AWS_OPERATOR_CIDR` fail before Terraform;
- an absent profile or any `AWS_PROFILE` other than `vals-dev` fails before AWS or Terraform;
- a returned partition other than `aws` is rejected;
- an account mismatch fails and prints both expected and actual IDs;
- deploy orders foundation apply, ECR publishing, workload apply, and readiness;
- test failure does not invoke destroy; and
- destroy orders workload destroy, namespace verification, foundation destroy, tag verification, then local secret cleanup.

Use a temporary `PATH` of recording shims; do not call AWS, Docker, Terraform, or Kubernetes in this unit test.

- [ ] **Step 2: Run the test and confirm the missing script failure**

Run:

```bash
uv run pytest tests/test_kubernetes_aws_script.py -q
```

Expected: failure because `infra/kubernetes/aws/kubernetes-aws` does not exist.

- [ ] **Step 3: Implement strict preflight and state paths**

Write the Bash script with `set -euo pipefail`, named functions, and a guarded `main`. Require the commands `aws`, `docker`, `kubectl`, `openssl`, `terraform`, and `uv`. Resolve defaults without overwriting system variables:

```bash
aws_region="${AWS_REGION:-us-east-2}"
deployment_name="${KUBERNETES_DEPLOYMENT_NAME:-cbs-kubernetes-smoke}"
dind_source_image="${DIND_SOURCE_IMAGE:-docker:28.3.3-dind}"
```

Validate the deployment name with `^[a-z][a-z0-9-]{2,31}$`, the operator CIDR with Python's `ipaddress.IPv4Network`, and the AWS identity using `aws sts get-caller-identity`. Reject any ARN not beginning with `arn:aws:` and any account unequal to `AWS_ACCOUNT_ID`.

Require `AWS_PROFILE` to equal `vals-dev`. Pass `--profile vals-dev` to every AWS CLI command, set the foundation AWS provider profile to `vals-dev`, and set `AWS_PROFILE=vals-dev` in Kubernetes and Helm EKS token exec blocks.

Use only these state paths after validating `deployment_name`:

```text
infra/kubernetes/aws/.state/${deployment_name}/foundation.tfstate
infra/kubernetes/aws/.state/${deployment_name}/workload.tfstate
infra/kubernetes/aws/.runtime/${deployment_name}/deployment.env
```

- [ ] **Step 4: Implement plan and deploy**

`plan` must initialize and validate both roots, then create a saved foundation plan using the deployment, region, and operator CIDR. It must report that the workload plan is created after foundation apply because the Kubernetes provider needs a live API endpoint.

`deploy` must:

1. run preflight;
2. apply the saved foundation plan;
3. log in to the output ECR repository;
4. build the control image with tag `control-$(git rev-parse --short=12 HEAD)` if that tag is absent;
5. pull `docker:28.3.3-dind`, tag it `dind-28.3.3`, and push it only if absent;
6. resolve both ECR digests with `aws ecr describe-images` and reject non-`sha256:` results;
7. generate a 64-character token with `openssl rand -hex 32` only when the mode-0600 runtime file is absent;
8. create a deployment-specific kubeconfig with `aws eks update-kubeconfig --profile vals-dev`, an explicit file path, and an explicit context alias;
9. derive exact digest prefixes for the benchmark repository and the stack ECR repository, then create and apply a saved workload plan with the absolute kubeconfig path/context, image digests, allowed prefixes, and API token passed through `TF_VAR_*`; and
10. wait for Cilium and the control Deployment to become ready.

Never print the API token or include it in a command argument.

- [ ] **Step 5: Implement port-forward, test, and destroy**

`port-forward` must run:

```bash
kubectl --context "$kube_context" -n benchmark-sandboxes \
  port-forward service/kubernetes-sandbox-control 8080:8080
```

`test` must start that port-forward in the background, wait for `/health`, export the local control URL/token and supplied benchmark image, run the live pytest file, and stop only the port-forward on exit. It must not call destroy.

`destroy` must:

1. load the runtime file without printing it;
2. destroy workload state;
3. confirm `benchmark-sandboxes` no longer exists;
4. destroy foundation state;
5. query the Resource Groups Tagging API for both mandatory tags and fail if any ARN remains; and
6. remove only the validated deployment's runtime and state directories after every residue check passes.

If workload destruction fails, stop before foundation destruction so the EKS API remains reachable.

- [ ] **Step 6: Add the infrastructure-local Makefile**

Create phony `plan`, `deploy`, `port-forward`, `test`, and `destroy` targets that invoke `./kubernetes-aws` with the matching command. Do not duplicate orchestration in Make recipes.

- [ ] **Step 7: Verify orchestration behavior**

Run:

```bash
uv run pytest tests/test_kubernetes_aws_script.py -q
uv run ruff check tests/test_kubernetes_aws_script.py
bash -n infra/kubernetes/aws/kubernetes-aws
git diff --check
```

Expected: all preflight and operation-order cases pass.

- [ ] **Step 8: Commit and push**

```bash
git add infra/kubernetes/aws/kubernetes-aws infra/kubernetes/aws/Makefile tests/test_kubernetes_aws_script.py
git commit -m "Orchestrate disposable EKS sandbox smoke"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 5: Tighten the live Kubernetes contract

**Files:**
- Modify: `tests/integration/test_kubernetes_control_service.py`

**Interfaces:**
- Consumes: `TEST_KUBERNETES_CONTROL_URL`, `TEST_KUBERNETES_CONTROL_TOKEN`, `TEST_KUBERNETES_IMAGE`, and optional `TEST_KUBERNETES_COMPOSE_IMAGE`.
- Produces: live proof for idempotency, real-time streaming, cancellation, file transfer, egress, Compose, timeouts, and cleanup.

- [ ] **Step 1: Add live gates before changing infrastructure behavior**

Extend the existing test rather than creating many test files. Use unique names and `finally` cleanup. Add cases that:

- create the same request twice and assert the same ID;
- consume the first WebSocket command chunk before the command's final sleep finishes;
- close a long-running command generator and verify its recorded PID no longer exists;
- upload at least 2 MiB of deterministic binary content, observe the first HTTP download chunk before reading the remainder, and compare all bytes;
- allow `example.com`, reject `example.org`, then clear egress and reach both;
- assert a command timeout returns the shared timeout error;
- when `TEST_KUBERNETES_COMPOSE_IMAGE` is set, wrap the created outer sandbox in `ComposeSandbox`, upload a Compose file with a `main` service, start it, stream a command, and tear it down; and
- always delete every created sandbox, including when an intermediate assertion fails.

Keep these cases in at most two tests: the required base contract and optional Compose contract.

- [ ] **Step 2: Run the local suite**

Run:

```bash
uv run pytest tests/integration/test_kubernetes_control_service.py -q
```

Expected before deployment: skipped because live variables are absent. During Task 7, each test must run rather than skip.

- [ ] **Step 3: Verify lint and type checking**

Run:

```bash
uv run ruff check tests/integration/test_kubernetes_control_service.py
uv run basedpyright tests/integration/test_kubernetes_control_service.py
```

Expected: no lint or type errors.

- [ ] **Step 4: Commit and push**

```bash
git add tests/integration/test_kubernetes_control_service.py
git commit -m "Expand live Kubernetes sandbox contract"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 6: Document the commercial smoke and root commands

**Files:**
- Modify: `Makefile`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the exact scripts, variables, outputs, and failure behavior from Tasks 1-5.
- Produces: one copyable commercial AWS workflow with explicit teardown.

- [ ] **Step 1: Add root Make aliases**

Add phony targets named `kubernetes-aws-plan`, `kubernetes-aws-deploy`, `kubernetes-aws-port-forward`, `kubernetes-aws-test`, and `kubernetes-aws-destroy`. Each target must delegate to `$(MAKE) -C infra/kubernetes/aws $(@:kubernetes-aws-%=%)`.

- [ ] **Step 2: Document prerequisites and safety checks**

Document Terraform 1.10+, AWS CLI, Docker, kubectl, uv, a commercial AWS profile, an approved account ID, operator `/32`, EKS/IAM/VPC/ECR permissions, the benchmark test image, expected EKS/NAT/EC2 charges, and the fact that the first smoke uses `runc` rather than Kata.

State plainly that Terraform builds only the control image. The benchmark provides the main sandbox image, while the pinned Docker daemon image is mirrored unchanged into ECR.

- [ ] **Step 3: Document the exact lifecycle**

Show these commands in order:

```bash
export AWS_PROFILE=vals-dev
export AWS_ACCOUNT_ID
export AWS_REGION=us-east-2
export AWS_OPERATOR_CIDR
export TEST_KUBERNETES_IMAGE

make kubernetes-aws-plan
make kubernetes-aws-deploy
make kubernetes-aws-test
make kubernetes-aws-destroy
```

Explain that failed tests do not auto-destroy, `port-forward` creates no public endpoint, and destroy must finish with both namespace and AWS tag residue checks empty.

- [ ] **Step 4: Verify docs and commands**

Run:

```bash
make -n kubernetes-aws-plan
make -n kubernetes-aws-deploy
make -n kubernetes-aws-test
make -n kubernetes-aws-destroy
git diff --check
```

Expected: every root target delegates to the infrastructure Makefile and documentation has no whitespace errors.

- [ ] **Step 5: Commit and push**

```bash
git add Makefile README.md docs/KUBERNETES_SANDBOX_PROVIDER.md
git commit -m "Document commercial EKS sandbox smoke"
git pull --rebase origin jf/test-kubernetes
git push origin jf/test-kubernetes
```

### Task 7: Verify locally, deploy once, test, and destroy

**Files:**
- No source changes expected. If live proof exposes a defect, return to the owning task, add one regression test, and make a separate focused commit.

**Interfaces:**
- Consumes: an explicitly confirmed commercial AWS account, operator CIDR, and digest-pinnable benchmark image.
- Produces: recorded command output proving apply, live contract, workload-first destroy, and zero tagged residue.

- [ ] **Step 1: Run the complete local verification once**

```bash
uv run pytest --ignore=tests/integration
uv run ruff check .
uv run basedpyright src/ tests/
terraform -chdir=infra/kubernetes/aws/foundation fmt -check -recursive
terraform -chdir=infra/kubernetes/aws/foundation validate
terraform -chdir=infra/kubernetes/workload fmt -check -recursive
terraform -chdir=infra/kubernetes/workload validate
bash -n infra/kubernetes/aws/kubernetes-aws
git diff --check
```

Expected: every command succeeds before AWS provisioning.

- [ ] **Step 2: Confirm the AWS identity without changing it**

Run:

```bash
aws --profile vals-dev sts get-caller-identity
aws --profile vals-dev configure get region
```

Run both commands with `--profile vals-dev`. Compare the returned account with `AWS_ACCOUNT_ID` and confirm the ARN begins with `arn:aws:`. Stop before apply if either check differs.

- [ ] **Step 3: Provision and capture the live proof**

```bash
make kubernetes-aws-plan
make kubernetes-aws-deploy
kubectl get nodes,pods -A
make kubernetes-aws-test
```

Expected: Cilium, the control service, and all required live contract cases pass. Do not report Kata, GPU, or GovCloud as tested.

- [ ] **Step 4: Destroy and verify absence**

```bash
make kubernetes-aws-destroy
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=Project,Values=create-benchmark-service-kubernetes Key=Deployment,Values=cbs-kubernetes-smoke
```

Expected: workload and foundation destroys succeed, the Kubernetes namespace check passes before cluster deletion, and `ResourceTagMappingList` is empty afterward.

- [ ] **Step 5: Record any final proof-only documentation update**

If the live run changes a documented command or supported version, update only those facts, run `git diff --check`, commit, pull, and push. Do not commit AWS account IDs, endpoints, tokens, kubeconfigs, Terraform state, or raw logs.

---

## Plan Self-Review

- Every approved design requirement maps to a task: image ownership in Tasks 1 and 3, two-layer Terraform in Tasks 2 and 3, ordered operations in Task 4, live gates in Task 5, setup documentation in Task 6, and provision/test/destroy proof in Task 7.
- The Terraform roots use separate state and EKS exec authentication consistently.
- The first smoke proves `runc`; Kata and GPU remain explicit later gates and are never reported as covered.
- No task imports shared AWS resources or creates a public control endpoint.
- The unrelated `tests/__init__.py` remains outside every staging command.
