# Kubernetes Spot Scale Profile Design

## Goal

Add a Spot-only EKS/Karpenter path for the existing 2,000-sandbox stress test without changing the current smoke or On-Demand scale profiles.

## Design

`KUBERNETES_SCALE_PROFILE=scale-2000-spot` reuses the capacity, namespace, control-service, NAT, and AMI settings from `scale-2000`, but passes only `spot` to the Karpenter NodePool. It does not fall back to On-Demand because a fallback would make cost and capacity results ambiguous.

The orchestration preflight queries the EC2 Standard Spot Instance Requests quota (`L-34B43A08`) and requires 5,000 vCPUs before Terraform runs. The existing `scale-2000` profile continues to query the Standard On-Demand quota (`L-1216C47A`). The selected capacity type is recorded with the other immutable plan inputs and passed to workload plan, apply, and destroy operations.

## Test Path

Use the existing stream soak with `TEST_KUBERNETES_SCALE_TARGET` set successively to 100, 500, and 2,000. Each run must clean up its Jobs before the next target, and the Terraform stack must be destroyed after testing.

The test remains a Kubernetes provider test: Batch and Fargate are out of scope. Spot interruptions are valid failures to record, not reasons to silently retry on On-Demand capacity.

## Alternatives Considered

- A free-form capacity-type environment override is more flexible, but it permits unreviewed combinations and makes saved-plan drift harder to understand.
- Allowing both Spot and On-Demand improves placement success, but it prevents a reliable Spot cost measurement.
- AWS Batch may be cheaper for finite jobs, but it does not exercise the Kubernetes sandbox lifecycle or streaming path.

## Verification

The shell orchestration test must prove that the Spot profile uses the Spot quota, rejects insufficient quota before Terraform, writes `spot` into the Terraform workload environment, and keeps On-Demand behavior unchanged. No AWS resources are provisioned by local verification.
