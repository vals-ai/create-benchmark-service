# Kubernetes Maintainability Cleanup Design

## Goal

Make the Kubernetes sandbox implementation easier to navigate and review without changing
its provider contract, deployment behavior, or tested safety guarantees.

## Terraform organization

Keep AWS foundation and workload composition under `infra/kubernetes/aws`. Split the
foundation into provider and shared locals, networking, EKS, Karpenter, and ECR files.

Create one reusable `infra/kubernetes/modules/sandbox-workload` module for the Kubernetes
namespace, quotas, runtime class, RBAC, baseline network policy, control service, and
janitor. The AWS workload root will install Cilium and Karpenter, then call that module
with AWS-specific scheduling, egress RBAC, and image values.

Moving existing Kubernetes resources into the module changes their Terraform addresses.
Add explicit `moved` blocks so an existing AWS workload state is upgraded rather than
recreated. A future GCP workload root can call the same module after installing its own
network and capacity components.

## Cloud portability

Keep the provider, client, control API, lifecycle backend, manifest construction, sandbox
agent, and control image independent of the cloud that runs the cluster.

Move the Cilium egress implementation behind the existing `EgressPolicyDriver` boundary
and select it through deployment settings. Move Karpenter Pod annotations out of generic
manifest construction and into deployment-provided Pod metadata. Node selectors, runtime
class, extra egress RBAC, and capacity dependencies must enter the shared workload module
as inputs.

AWS-specific code includes EKS, IAM, VPC, ECR, Karpenter, AWS CNI chaining, commercial
partition checks, and the `vals-dev` wrapper. A future GCP package will provide GKE,
Artifact Registry, node capacity, identity, and an egress implementation without changing
the sandbox provider API.

## Future observability

Keep telemetry outside the sandbox provider contract. A later observability change can
export standard OpenTelemetry metrics, logs, and traces from the control service, sandbox
agent, and cluster components to Grafana Cloud, including the Asserts entity view, without
making Grafana a runtime dependency.

## AWS orchestration

Keep `infra/kubernetes/aws/kubernetes-aws` as the command entrypoint. Move profile values,
runtime-state handling, deploy helpers, and destroy helpers into focused sourced files.
Plan, deploy, and destroy must continue to receive the same Terraform inputs.

The entrypoint must continue to reject profiles other than `vals-dev`, reject the wrong AWS
account or partition, preserve state after partial failures, and remove state only after
destroy and residue verification succeed.

## Essential AWS test coverage

Replace the current monolithic orchestration unit test with one small process-level test
covering only behavior that is expensive or unsafe to prove on every live run:

- The wrapper rejects a non-`vals-dev` profile and wrong account before Terraform changes.
- A failed destroy preserves runtime and Terraform state so the operator can retry.

Remove source-text assertions, per-setting snapshots, and simulated coverage already proved
by Terraform validation, shell syntax checks, or the live Kubernetes contract.

## Python and Go organization

Split the FastAPI application factory into application wiring, HTTP routes, WebSocket
compatibility, and error handling. Split Kubernetes Job construction into named agent,
Docker, sandbox-container, Pod-spec, and Job-metadata phases.

Keep the Go agent HTTP surface unchanged. Separate command process creation, output
collection, event streaming, and exit reporting so cancellation and trailing-output
behavior remain visible in the main flow.

Keep the existing lifecycle backend, resource cache, and data-plane boundaries. Extract
additional helpers only when they remove a repeated phase or isolate state with its own
lifecycle.

## Comments and docstrings

Inline comments must be one line, with no more than two inside any function. Outside
functions, use no more than two comments in one logical section. Comments explain
non-obvious invariants such as watch recovery, process-group cancellation, partial
deployment recovery, or residue-safe destroy; they do not narrate assignments.

Module and public-boundary docstrings are one or two sentences. Public methods document
side effects, retries, cleanup, streaming, or raised errors only when the signature does not
make those behaviors clear.

## Verification

The cleanup is complete when:

- Ruff and basedpyright pass.
- Non-integration pytest tests pass.
- The retained AWS orchestration safety test passes.
- Both AWS Terraform roots and the shared workload module pass formatting and validation.
- The Karpenter Helm chart passes lint.
- The Go sandbox agent tests pass.
- The AWS wrapper and sourced shell files pass `bash -n`.
- Existing AWS Terraform state has explicit moves for shared module resources.
- No provider API, HTTP/NDJSON protocol, or AWS deploy/destroy behavior changes.

## Non-goals

This cleanup does not deploy infrastructure, provision GCP, configure Grafana Cloud, change
scale profiles, introduce a new runtime, change sandbox isolation, or expand test coverage.
