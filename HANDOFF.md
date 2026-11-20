# Agent handoff

Snapshot: November20,2026,12:59PM Pacific. Refresh AWS status before acting.

## Current operation

- Authorized full Silver run `silver_full_20261119_r2`: all378 transformations completed; `audit_committed_partitions` running; no failed tasks. Completion is not yet verified.
- Account498768607823, profile `gpu-fleet`, region `us-east-2`; worker `i-09dc0bfe45b0d275f`, m6a.2xlarge,60-GiB gp3. Temporary500-GiB volume deleted.
- Bucket `gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6`; live report `run-logs/full-silver-20261119/status.json`. Check freshness, final `verified_complete`, `run_outcome`, and service result before declaring success.
- Airflow3.0.6/LocalExecutor/PostgreSQL15; isolated Silver-only DAG folder. Spark local[6],12-GiB heap. API binds worker loopback8080; local dashboard requires an SSM tunnel.
- `/opt/gpu-fleet/production.env.sh` contains private configuration. Legacy `env.sh` has pilot limits. Never publish secrets, Airflow database dumps or authentication files.
- Supervisor: minute health checks, two-hour checkpoints,10-GiB disk floor,30-hour internal cap,31-hour service cap,34-hour boot guard; independent180-second finalizer shutdown watchdog. Leave healthy processing running.

## Recovery and semantics

Initial attempt failed on pod day5/hour06 physical schema drift. Full9,049-footer preflight and12 recovery tests passed; native-schema safe numeric widening and typed null restoration fixed it. The failing day and its join audit passed before R2. A finalizer dependency cycle was repaired and independent watchdog behavior tested.

Day5/hour06 has zero requests but positive GPU activity: preserve/flag, do not infer idle capacity. Current code SHA is recorded in saved Silver manifests. Old code/benchmark attempts remain historical. Read current accepted paths only, never wildcard versions.

## Next actions

1. Verify all378 manifests and192 final audit bindings; save final status and the Gold handoff outcome under `docs/evidence/`; capture the stopped EC2 snapshot after Gold finishes. Refresh the evidence checksum inventory and README/HANDOFF status together.
2. Gold execution is authorized and deployed: see [Gold runbook](docs/runbooks/GOLD_RUNBOOK.md). A success-only Silver finalizer handoff is armed. Gold service explicitly waits for Silver finalization; it then runs the four-day pilot,12-hour projection gate and full Gold release/export. Do not duplicate the queued run or stop a healthy worker. Gold internal/service limits15/16hours,10-GiB disk floor and independent shutdown guards apply. New Gold release ID `gold_20261120`; status under `run-logs/gold/gold_20261120/`.
3. Export compact Gold bundle and validate Streamlit without AWS before any cloud teardown. Deletion requires the user's teardown instruction.
4. The user authorized commit/push. Documentation/Silver were pushed at `d8fe8b8` on `gpu-fleet-analytics`; Gold implementation/deployment are included in the subsequent commit. Do not claim main was merged.

## Navigation

[Evidence](docs/evidence/README.md) · [Data contracts](docs/data/COLUMN_CONTRACTS.md) · [Silver runbook](docs/runbooks/SILVER_RUNBOOK.md) · [Gold plan](docs/design/GOLD_TRANSFORMATION_PLAN.md). Historical operations and local helper paths remain in [archived handoff](docs/archive/HANDOFF-history.md).

Repository was renamed from Data-Eng to gpu-fleet-analytics. Main CloudFormation still reflects historical t3.large/30-GiB defaults: do not overwrite approved live sizing blindly. Credits are shared/delayed; no remaining-balance guarantee.
