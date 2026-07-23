# Kubernetes Maintainability Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the Kubernetes sandbox implementation into readable files and flows while preserving its provider, protocol, infrastructure, and recovery behavior.

**Architecture:** Keep the provider/control protocol cloud-neutral, move reusable cluster resources into one Terraform module, and keep EKS, Karpenter, Cilium configuration, and AWS orchestration under `infra/kubernetes/aws`. Split large files by responsibility, keep public entrypoints stable, and retain one process-level AWS safety test instead of implementation snapshots.

**Tech Stack:** Python 3.12, FastAPI, pytest, Ruff, basedpyright, Terraform, Bash, Helm, Go.

## Global Constraints

- Do not change the sandbox provider API, HTTP/NDJSON protocol, scale-profile values, or AWS deploy/destroy behavior.
- Preserve existing AWS Terraform resources with explicit `moved` blocks when their addresses enter the shared module.
- Keep each inline comment to one line and use no more than two inside any function.
- Outside functions, use no more than two inline comments in one logical section.
- Keep module and public-boundary docstrings to one or two sentences.
- Keep cloud infrastructure in provider folders and create only one reusable child module for the sandbox workload.
- Keep only AWS account/profile protection and recoverable-destroy behavior in the process-level AWS test.
- Do not deploy infrastructure.
- Do not configure Grafana Cloud or add a telemetry dependency in this refactor.
- Do not commit or push without explicit user authorization.

---

### Task 1: Separate shared Kubernetes resources from AWS composition

**Files:**
- Create: `infra/kubernetes/aws/foundation/networking.tf`
- Create: `infra/kubernetes/aws/foundation/eks.tf`
- Create: `infra/kubernetes/aws/foundation/capacity.tf`
- Create: `infra/kubernetes/aws/foundation/registry.tf`
- Modify: `infra/kubernetes/aws/foundation/main.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/versions.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/variables.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/namespace.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/control.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/janitor.tf`
- Create: `infra/kubernetes/modules/sandbox-workload/outputs.tf`
- Create: `infra/kubernetes/aws/workload/main.tf`
- Create: `infra/kubernetes/aws/workload/cilium.tf`
- Create: `infra/kubernetes/aws/workload/capacity.tf`
- Create: `infra/kubernetes/aws/workload/moved.tf`
- Create: `infra/kubernetes/aws/workload/variables.tf`
- Create: `infra/kubernetes/aws/workload/outputs.tf`
- Create: `infra/kubernetes/aws/workload/versions.tf`
- Move: `infra/kubernetes/workload/.terraform.lock.hcl` to `infra/kubernetes/aws/workload/.terraform.lock.hcl`
- Delete: `infra/kubernetes/workload/main.tf`
- Delete: `infra/kubernetes/workload/variables.tf`
- Delete: `infra/kubernetes/workload/outputs.tf`
- Delete: `infra/kubernetes/workload/versions.tf`
- Modify: `infra/kubernetes/aws/kubernetes-aws`
- Modify: `docs/KUBERNETES_SANDBOX_PROVIDER.md`

**Interfaces:**
- Consumes: Existing variables, providers, resource names, and module names.
- Produces: An AWS composition root and a reusable cluster workload module with state-safe resource moves.

- [ ] **Step 1: Record the resource-address baseline**

Run:

```bash
terraform -chdir=infra/kubernetes/aws/foundation providers
terraform -chdir=infra/kubernetes/workload providers
```

Expected: both commands exit successfully and show the currently initialized providers.

- [ ] **Step 2: Split the foundation root**

Leave only the AWS provider, Availability Zone data source, and shared locals in `main.tf`.
Move complete blocks without renaming them:

```text
networking.tf
  aws_iam_service_linked_role.ec2_spot
  module.vpc
  aws_security_group.vpc_endpoints
  module.vpc_endpoints

eks.tf
  module.eks
  every aws_security_group_rule attached to EKS or node groups

capacity.tf
  module.karpenter

registry.tf
  aws_ecr_repository.sandbox
```

- [ ] **Step 3: Create the shared workload module**

Move these complete Kubernetes resources into `infra/kubernetes/modules/sandbox-workload`:

```text
namespace.tf
  kubernetes_namespace_v1.sandboxes
  kubernetes_runtime_class_v1.runc
  kubernetes_resource_quota_v1.sandboxes
  kubernetes_limit_range_v1.sandboxes
  kubernetes_service_account_v1.control
  kubernetes_role_v1.control
  kubernetes_role_binding_v1.control
  kubernetes_network_policy_v1.sandbox_ingress

control.tf
  kubernetes_secret_v1.control
  kubernetes_deployment_v1.control
  kubernetes_pod_disruption_budget_v1.control
  kubernetes_service_v1.control

janitor.tf
  kubernetes_cron_job_v1.sandbox_janitor
```

Expose the existing namespace, service name, and runtime-class outputs. Accept all existing
cluster-neutral values plus these deployment inputs:

```hcl
variable "sandbox_node_selector" {
  type = map(string)
}

variable "sandbox_pod_annotations" {
  type = map(string)
}

variable "runtime_class_name" {
  type = string
}

variable "runtime_class_handler" {
  type = string
}

variable "egress_driver" {
  type = string
}

variable "egress_rbac_rules" {
  type = list(object({
    api_groups = list(string)
    resources  = list(string)
  }))
}
```

Render map settings as comma-separated `key=value` environment values. Create additional
Role rules with a `dynamic "rule"` block. Use the runtime-class inputs for the
`kubernetes_runtime_class_v1` resource. Keep current AWS values at the caller so the module
itself contains no Karpenter, EKS, ECR, or AWS CNI names.

- [ ] **Step 4: Create the AWS workload composition root**

Move both providers to `infra/kubernetes/aws/workload/main.tf`. Keep shared workload locals
inside the module. Keep `helm_release.cilium` in `cilium.tf` and both Karpenter releases in
`capacity.tf`. Call the shared module only after the AWS add-ons are ready:

```hcl
module "sandbox_workload" {
  source = "../../modules/sandbox-workload"

  sandbox_node_selector = {
    "karpenter.sh/nodepool" = "sandbox"
  }
  sandbox_pod_annotations = {
    "karpenter.sh/do-not-disrupt" = "true"
  }
  runtime_class_name    = "runc"
  runtime_class_handler = "runc"
  egress_driver = "cilium"
  egress_rbac_rules = [{
    api_groups = ["cilium.io"]
    resources  = ["ciliumnetworkpolicies"]
  }]

  depends_on = [helm_release.cilium, helm_release.karpenter_capacity]
}
```

Pass every existing workload variable through unchanged. Update the local chart source to
`../../charts/karpenter-sandbox`.

- [ ] **Step 5: Preserve existing AWS Terraform state**

Add a `moved` block for every Kubernetes resource moved into the module. For example:

```hcl
moved {
  from = kubernetes_namespace_v1.sandboxes
  to   = module.sandbox_workload.kubernetes_namespace_v1.sandboxes
}
```

Keep Helm release addresses unchanged. Change the wrapper to:

```bash
workload_root="$script_dir/workload"
```

The existing `workload.tfstate` file remains in the same deployment state directory.

- [ ] **Step 6: Update documentation paths**

Document the three boundaries:

```text
infra/kubernetes/modules/sandbox-workload  shared Kubernetes resources
infra/kubernetes/aws/foundation           EKS, VPC, IAM, and ECR
infra/kubernetes/aws/workload             AWS add-ons plus shared module composition
```

State that a future GCP root reuses the module but supplies GKE capacity, registry,
scheduling metadata, and its egress driver.

- [ ] **Step 7: Format and validate both roots and the module**

Run:

```bash
terraform fmt -recursive infra/kubernetes/aws infra/kubernetes/modules
terraform -chdir=infra/kubernetes/aws/foundation validate
terraform -chdir=infra/kubernetes/aws/workload init -backend=false
terraform -chdir=infra/kubernetes/aws/workload validate
```

Expected: both validation commands report `Success! The configuration is valid.`

- [ ] **Step 8: Review the structural diff**

Run:

```bash
git diff --check
git diff -- infra/kubernetes/aws infra/kubernetes/modules docs/KUBERNETES_SANDBOX_PROVIDER.md
```

Expected: complete resource blocks move, AWS values leave the shared module, Helm release
addresses remain unchanged, and each moved Kubernetes resource has an explicit state move.

- [ ] **Step 9: Prepare the Terraform commit only after authorization**

```bash
git add infra/kubernetes/aws infra/kubernetes/modules docs/KUBERNETES_SANDBOX_PROVIDER.md
git commit -m "refactor(kubernetes): separate shared cluster workload"
```

### Task 2: Retain one readable AWS safety test

**Files:**
- Create: `tests/fixtures/kubernetes_aws_mock_command.py`
- Modify: `tests/test_kubernetes_aws_script.py`
- Test: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: `infra/kubernetes/aws/kubernetes-aws` as an external process.
- Produces: One behavioral test for account/profile rejection and destroy-state preservation.

- [ ] **Step 1: Replace the embedded shell emulator with one executable mock**

Create an executable Python dispatcher whose command name comes from `Path(sys.argv[0]).name`:

```python
#!/usr/bin/env python3
"""Provide deterministic command responses for the AWS wrapper safety test."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def record() -> str:
    command = f"{Path(sys.argv[0]).name} {' '.join(sys.argv[1:])}"
    with Path(os.environ["MOCK_COMMAND_LOG"]).open("a") as log:
        log.write(f"{command}\n")

    return command


def main() -> int:
    command = record()
    name = Path(sys.argv[0]).name
    if name == "aws" and "sts get-caller-identity" in command:
        print(
            os.environ.get("MOCK_AWS_ACCOUNT", "123456789012"),
            os.environ.get("MOCK_AWS_ARN", "arn:aws:iam::123456789012:user/tester"),
            sep="\t",
        )
    elif name == "terraform" and " destroy " in f" {command} ":
        if "workload.tfstate" in command and os.environ.get("MOCK_WORKLOAD_DESTROY_FAIL") == "1":
            return 1
    elif name == "kubectl" and "port-forward" in command:
        while True:
            time.sleep(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

The dispatcher must implement only these successful setup responses: AWS identity and
service quota, ECR image lookup and login, Terraform init/validate/plan/apply/output/destroy,
OpenSSL token generation, Git revision lookup, Docker build/push, kubectl readiness and
port-forward, curl health checks, and `uv run python`. The workload destroy branch returns
nonzero only when `MOCK_WORKLOAD_DESTROY_FAIL=1`. Do not reintroduce source-text assertions
or profile-setting snapshots.

- [ ] **Step 2: Write one process-level safety test**

Use `tmp_path` to copy the dispatcher under the required command names and run the wrapper:

```python
def test_aws_wrapper_preserves_identity_and_destroy_guards(tmp_path: Path) -> None:
    """Keep the account boundary and failed-destroy recovery state intact.

    Test cases:
    - A non-vals-dev profile and wrong account stop before Terraform.
    - A failed workload destroy preserves state for a retry.
    """
```

The test must assert observable exit status, stderr, command log, and state-directory
existence. It must not read Terraform or shell source files.

- [ ] **Step 3: Run the focused test before shell changes**

Run:

```bash
uv run pytest tests/test_kubernetes_aws_script.py -q
```

Expected: one test passes.

- [ ] **Step 4: Check the trim**

Run:

```bash
wc -l tests/test_kubernetes_aws_script.py tests/fixtures/kubernetes_aws_mock_command.py
```

Expected: the combined focused test and mock are substantially smaller than the former
827-line test and contain no implementation-source snapshots.

- [ ] **Step 5: Prepare the test commit only after authorization**

```bash
git add tests/test_kubernetes_aws_script.py tests/fixtures/kubernetes_aws_mock_command.py
git commit -m "test(kubernetes): retain AWS recovery safeguards"
```

### Task 3: Split the AWS orchestration entrypoint

**Files:**
- Create: `infra/kubernetes/aws/lib/profiles.sh`
- Create: `infra/kubernetes/aws/lib/runtime.sh`
- Create: `infra/kubernetes/aws/lib/deploy.sh`
- Create: `infra/kubernetes/aws/lib/destroy.sh`
- Modify: `infra/kubernetes/aws/kubernetes-aws`
- Test: `tests/test_kubernetes_aws_script.py`

**Interfaces:**
- Consumes: Existing exported environment variables and shared shell variables.
- Produces: The same `plan`, `deploy`, `port-forward`, `test`, and `destroy` commands.

- [ ] **Step 1: Source focused function libraries**

After the shared paths and defaults are defined, source exact repository-controlled paths:

```bash
source "$script_dir/lib/profiles.sh"
source "$script_dir/lib/runtime.sh"
source "$script_dir/lib/deploy.sh"
source "$script_dir/lib/destroy.sh"
```

- [ ] **Step 2: Move profile and quota behavior**

Move `configure_scale_profile` and `check_scale_quotas` unchanged to `lib/profiles.sh`.
Retain every current scale value and the pinned-AMI validation.

- [ ] **Step 3: Move runtime-state behavior**

Move these functions unchanged to `lib/runtime.sh`:

```text
configure_paths
write_plan_inputs
require_unchanged_plan_inputs
write_foundation_runtime_contents
write_foundation_runtime_file
transition_to_foundation_runtime
write_runtime_file
transition_to_workload_runtime
load_runtime_file
require_workload_runtime
```

Keep the atomic temporary-file rename and mode `0600` behavior.

- [ ] **Step 4: Move plan/deploy behavior**

Move Terraform initialization, foundation planning, ECR helpers, deployment, port-forward,
and live-test functions to `lib/deploy.sh`. Extract one shared workload environment function:

```bash
run_workload_terraform() {
  local operation="$1"
  shift

  TF_VAR_kubeconfig_path="$kubeconfig_path" \
    TF_VAR_kubeconfig_context="$kube_context" \
    TF_VAR_cluster_name="$cluster_name" \
    TF_VAR_api_token="$api_token" \
    terraform -chdir="$workload_root" "$operation" "$@"
}
```

Include every existing `TF_VAR_*` assignment in this function so plan, apply, and destroy
cannot drift.

- [ ] **Step 5: Move destroy behavior**

Move `tagged_resource_is_live`, `destroy_foundation`, and `destroy_stack` to
`lib/destroy.sh`. Keep residue verification before state-directory deletion.

- [ ] **Step 6: Keep the entrypoint as validation and dispatch**

Leave `fail`, required-command/input checks, AWS identity validation, `preflight`, and
`main` in `kubernetes-aws`. Do not change its command names or defaults.

- [ ] **Step 7: Validate and run the safety test**

Run:

```bash
bash -n infra/kubernetes/aws/kubernetes-aws infra/kubernetes/aws/lib/*.sh
uv run pytest tests/test_kubernetes_aws_script.py -q
```

Expected: shell syntax passes and the single safety test passes.

- [ ] **Step 8: Prepare the shell commit only after authorization**

```bash
git add infra/kubernetes/aws tests/test_kubernetes_aws_script.py tests/fixtures/kubernetes_aws_mock_command.py
git commit -m "refactor(kubernetes): split AWS orchestration"
```

### Task 4: Split FastAPI application wiring from routes

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/errors.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/streaming.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/http_routes.py`
- Create: `src/benchmark_service/sandbox/kubernetes/control/websocket_routes.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/app.py`
- Test: `tests/test_kubernetes_control_app.py`

**Interfaces:**
- Consumes: `KubernetesControlSettings`, `SandboxControlBackend`, and existing protocol models.
- Produces: The unchanged `create_kubernetes_control_app(...) -> FastAPI` entrypoint and routes.

- [ ] **Step 1: Preserve the public HTTP and WebSocket contract**

Run:

```bash
uv run pytest tests/test_kubernetes_control_app.py -q
```

Expected: all existing app contract tests pass before moving code.

- [ ] **Step 2: Move request and error handling**

Create `errors.py` with these interfaces:

```python
def request_id(headers: Mapping[str, str]) -> str: ...
def authorized(authorization: str | None, token: str) -> bool: ...
def error_detail(error: SandboxError, request_id: str) -> tuple[int, ControlErrorDetail]: ...
def error_response(status_code: int, detail: ControlErrorDetail) -> JSONResponse: ...
def install_http_error_handling(app: FastAPI) -> None: ...
```

Move behavior without changing status codes, error codes, or `X-Request-ID`.

- [ ] **Step 3: Move NDJSON streaming**

Create `streaming.py` and move:

```python
async def command_events_to_ndjson(
    stream: AsyncGenerator[CommandEvent, None],
    *,
    request_id: str,
    heartbeat_seconds: float,
) -> AsyncGenerator[bytes, None]:
    ...
```

Retain blank heartbeats, terminal-event enforcement, error events, cancellation, and
upstream generator closure.

- [ ] **Step 4: Create the HTTP router**

Create:

```python
def create_http_router(
    settings: KubernetesControlSettings,
    backend: SandboxControlBackend,
    readiness: Callable[[], Awaitable[bool]] | None,
) -> APIRouter:
    ...
```

Move health, readiness, lifecycle, exec, streaming command, files, and egress handlers.
Keep authentication and validation behavior unchanged.

- [ ] **Step 5: Create the compatibility WebSocket router**

Create:

```python
def create_websocket_router(
    settings: KubernetesControlSettings,
    backend: SandboxControlBackend,
) -> APIRouter:
    ...
```

Move only the WebSocket command route and its cancellation/error behavior.

- [ ] **Step 6: Reduce `app.py` to wiring**

`create_kubernetes_control_app` should build the lifespan, install error handling, and
include both routers:

```python
app = FastAPI(lifespan=lifespan)
install_http_error_handling(app)
app.include_router(create_http_router(settings, backend, readiness))
app.include_router(create_websocket_router(settings, backend))
return app
```

- [ ] **Step 7: Run app and static checks**

Run:

```bash
uv run pytest tests/test_kubernetes_control_app.py -q
uv run ruff check src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
uv run basedpyright src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
```

Expected: every command passes.

- [ ] **Step 8: Prepare the route commit only after authorization**

```bash
git add src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_control_app.py
git commit -m "refactor(kubernetes): separate control routes"
```

### Task 5: Make Job construction cloud-neutral and readable

**Files:**
- Create: `src/benchmark_service/sandbox/kubernetes/control/cilium_egress.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/egress.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/main.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/resources.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/settings.py`
- Modify: `tests/test_kubernetes_control_main.py`
- Test: `tests/test_kubernetes_resources.py`
- Test: `tests/test_kubernetes_backend.py`

**Interfaces:**
- Consumes: Deployment-selected scheduling metadata and egress driver settings.
- Produces: The unchanged AWS Job manifest and a cloud-neutral manifest builder.

- [ ] **Step 1: Preserve AWS manifest and egress behavior**

Run:

```bash
uv run pytest tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_main.py -q
```

Expected: deterministic security, scheduling, and Cilium egress behavior passes.

- [ ] **Step 2: Move cloud scheduling metadata into settings**

Add cloud-neutral settings:

```python
sandbox_pod_annotations: dict[str, str] = Field(default_factory=dict)
egress_driver: str = "cilium"
```

Load `KUBERNETES_SANDBOX_POD_ANNOTATIONS` with the existing `_mapping` parser and load
`KUBERNETES_SANDBOX_EGRESS_DRIVER` as a required nonempty setting. Build Pod annotations
from `settings.sandbox_pod_annotations`; remove the literal
`karpenter.sh/do-not-disrupt` key from `resources.py`.

The AWS workload module supplies the existing Karpenter annotation, so the EKS manifest
does not change. Add one setting-loader case and adjust the existing resource case instead
of adding new test functions.

- [ ] **Step 3: Isolate the Cilium egress implementation**

Keep `EgressPolicyDriver` in `egress.py`. Move `build_egress_policy` and
`CiliumEgressPolicyDriver` to `cilium_egress.py`, then add:

```python
def create_egress_driver(
    settings: KubernetesControlSettings,
    api: KubernetesApi,
) -> EgressPolicyDriver:
    if settings.egress_driver == "cilium":
        return CiliumEgressPolicyDriver(settings, api)
    raise ValueError(f"Unsupported Kubernetes egress driver: {settings.egress_driver}")
```

Use the factory from `main.py`. This keeps Cilium as the current AWS implementation while
leaving one explicit extension point for a future GCP implementation.

- [ ] **Step 4: Extract named manifest builders**

Use focused functions with raw Kubernetes dictionaries only at this boundary:

```python
def _build_agent_init_container(settings: KubernetesControlSettings) -> dict[str, object]: ...
def _build_docker_sidecar(settings: KubernetesControlSettings) -> dict[str, object]: ...
def _build_sandbox_container(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    source: ImageSource,
    resource_name: str,
) -> dict[str, object]: ...
def _build_pod_spec(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    source: ImageSource,
    resource_name: str,
) -> dict[str, object]: ...
def _build_annotations(request: SandboxCreateRequest, now: datetime) -> dict[str, str]: ...
```

`build_job` should validate, name, label, call these phases, and assemble the Job.

- [ ] **Step 5: Add only invariant comments**

Add at most two one-line comments in any builder. Valid subjects are the injected agent
binary, privileged Docker sidecar boundary, and deployment-provided scheduling metadata.

- [ ] **Step 6: Verify exact behavior**

Run:

```bash
uv run pytest tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_main.py -q
uv run ruff check src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_main.py
uv run basedpyright src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_main.py
```

Expected: all commands pass.

- [ ] **Step 7: Prepare the resource commit only after authorization**

```bash
git add src/benchmark_service/sandbox/kubernetes/control tests/test_kubernetes_resources.py tests/test_kubernetes_backend.py tests/test_kubernetes_control_main.py
git commit -m "refactor(kubernetes): isolate cluster-specific settings"
```

### Task 6: Separate Go command execution from event streaming

**Files:**
- Modify: `infra/kubernetes/agent/main.go`
- Test: `infra/kubernetes/agent/main_test.go`

**Interfaces:**
- Consumes: Existing `/v1/command` JSON and request context.
- Produces: Unchanged NDJSON stdout, stderr, heartbeat, and exit events.

- [ ] **Step 1: Preserve agent behavior**

Run:

```bash
GOCACHE=/tmp/create-benchmark-service-go-cache go test ./...
```

Working directory: `infra/kubernetes/agent`.

Expected: all agent tests pass.

- [ ] **Step 2: Introduce a running-command boundary**

Create:

```go
type runningCommand struct {
	command     *exec.Cmd
	stdout      io.ReadCloser
	stderr      io.ReadCloser
	cancel      context.CancelFunc
	context     context.Context
	processDone chan struct{}
}

func startCommand(request *http.Request, payload commandRequest) (*runningCommand, error)
func (command *runningCommand) close()
```

Keep environment, working directory, timeout, and process-group behavior unchanged.

- [ ] **Step 3: Extract event collection and response streaming**

Create:

```go
func collectCommandEvents(command *runningCommand) (<-chan commandEvent, <-chan error)
func streamCommandEvents(
	response http.ResponseWriter,
	command *runningCommand,
	events <-chan commandEvent,
	waitResult <-chan error,
	heartbeat time.Duration,
)
```

`handleCommand` should decode, start, collect, and stream in that order.

- [ ] **Step 4: Add concise invariant comments**

Use no more than two one-line comments in each new function. Explain only that readers
finish before the exit event and cancellation targets the process group.

- [ ] **Step 5: Format and verify**

Run:

```bash
gofmt -w main.go main_test.go
GOCACHE=/tmp/create-benchmark-service-go-cache go test ./...
```

Working directory: `infra/kubernetes/agent`.

Expected: all tests pass, including cancellation, heartbeats, concurrency, and UTF-8.

- [ ] **Step 6: Prepare the Go commit only after authorization**

```bash
git add infra/kubernetes/agent
git commit -m "refactor(kubernetes): clarify agent streaming"
```

### Task 7: Add targeted documentation and enforce the comment rule

**Files:**
- Modify: `src/benchmark_service/sandbox/kubernetes/*.py`
- Modify: `src/benchmark_service/sandbox/kubernetes/control/*.py`
- Modify: `/Users/jarettforzano/.codex/skills/backend/references/python-standards.md`

**Interfaces:**
- Consumes: Existing public classes, functions, and behavior.
- Produces: Short module/public-boundary documentation without code-flow narration.

- [ ] **Step 1: Add module docstrings**

Add one sentence to each Kubernetes Python module and both package `__init__.py` files.
Describe the module boundary, not its file name.

- [ ] **Step 2: Document public boundaries**

Add one or two sentences to public provider, sandbox, client, control backend, API, cache,
transport, and settings methods only when side effects, retries, cleanup, or streaming are
not clear from the signature. Do not add boilerplate to every Protocol method.

- [ ] **Step 3: Add only high-value inline comments**

Use one-line comments, no more than two inside any function, for:

```text
expired Kubernetes watch history triggers a full relist
revision changes wake shared readiness waiters
incomplete command streams terminate the process group
runtime phases preserve partial-deploy recovery
state deletion follows residue verification
```

- [ ] **Step 4: Update the backend Python reference**

Add this rule to the formatting/comment guidance:

```markdown
- Keep inline comments to one readable line and use at most two inside a function.
- Outside functions, use at most two inline comments in one logical section.
- Comment non-obvious invariants and intent; do not narrate assignments or control flow.
```

- [ ] **Step 5: Verify documentation and formatting**

Run:

```bash
uv run ruff format src/benchmark_service/sandbox/kubernetes
uv run ruff check src/benchmark_service/sandbox/kubernetes
uv run basedpyright src/benchmark_service/sandbox/kubernetes
.venv/bin/python /Users/jarettforzano/.codex/skills/.system/skill-creator/scripts/quick_validate.py /Users/jarettforzano/.codex/skills/backend
```

Expected: all commands pass and the skill reports `Skill is valid!`

- [ ] **Step 6: Prepare the documentation commit only after authorization**

```bash
git add src/benchmark_service/sandbox/kubernetes
git commit -m "docs(kubernetes): explain control invariants"
```

### Task 8: Run final cross-language verification

**Files:**
- Verify all files changed by Tasks 1-7.

**Interfaces:**
- Consumes: The complete cleanup.
- Produces: Evidence that behavior and infrastructure validation remain intact.

- [ ] **Step 1: Run Python checks**

```bash
UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run ruff check .
UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run basedpyright src/ tests/
UV_CACHE_DIR=/tmp/create-benchmark-service-uv-cache uv run pytest -q --ignore=tests/integration
```

Expected: Ruff and basedpyright pass; all non-integration tests pass.

- [ ] **Step 2: Run infrastructure checks**

```bash
terraform fmt -check -recursive infra/kubernetes/aws infra/kubernetes/modules
terraform -chdir=infra/kubernetes/aws/foundation validate
terraform -chdir=infra/kubernetes/aws/workload validate
helm lint infra/kubernetes/charts/karpenter-sandbox
bash -n infra/kubernetes/aws/kubernetes-aws infra/kubernetes/aws/lib/*.sh
```

Expected: both Terraform roots are valid, the chart has no failures, and shell syntax passes.

- [ ] **Step 3: Run Go checks**

```bash
GOCACHE=/tmp/create-benchmark-service-go-cache go test ./...
```

Working directory: `infra/kubernetes/agent`.

Expected: all Go tests pass.

- [ ] **Step 4: Review scope and comments**

```bash
git diff --check
rg -n '^\s*(#|//)' src/benchmark_service/sandbox/kubernetes infra/kubernetes/agent infra/kubernetes/aws
git status -sb
```

Expected: comments are one line and limited to non-obvious invariants; no unrelated files are staged.

- [ ] **Step 5: Present the verified diff for authorization**

Do not commit or push. Report the exact changed files, checks, and any live integration work
that remains intentionally unrun.
