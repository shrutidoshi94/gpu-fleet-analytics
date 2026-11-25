# Agent handoff

Snapshot: November23,2026. Verified local Gold and offline Streamlit dashboard implemented. EC2 confirmed stopped.

## Gold dictionary

Read docs/data/GOLD_DATA_DICTIONARY.md for every column and docs/data/GOLD_METADATA_PROFILE.json for measured metadata. Seven tables/86 field occurrences; all grouping grains verified unique. Requests are hourly observations, not proven simultaneous running demand. Unknown lifecycle covers99.001% of raw observed request weight; avoid calling this confirmed running demand.

## Local dashboard

- Production bundle: `data/gold/gold_20261120/` (gitignored). Second downloaded copy: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/work/gold-download-20261123/`. Both are on this laptop.
- Streamlit: `dashboard.py`, pure data/math helpers `dashboard_data.py`; server127.0.0.1:8501. Minimal runtime `requirements-dashboard.txt`; no AWS/Java/Spark needed to serve the saved data.
- Dashboard redesigned at user request: one three-step story (Find the pressure / Review existing capacity / Test the requirement), short single-line headings, one selected cohort throughout. Optional quality/mix/network/execution/AWS details are collapsed. Real-data story smoke and offline tests passed; original synthetic Spark/JSON pilot is preserved.
- Local validation:9 hashes, seven schemas/row counts,185 inventory totals and five mix populations matching6,570,124,684 pod observations. Saved under docs/evidence/. No raw-trace reprocessing was done during local validation.
- Sidebar screens flagged request evidence by default;53 days have unproven duplicate candidates. Preserve source anomaly and unknown semantics. Scenario calculations remain exploratory and single-cohort.

## Current operation

- Silver `silver_full_20261119_r2` is verified complete:378 partitions,192 audit bindings,6,759,480,015 accepted rows. Gold `gold_20261120` is verified complete:185 daily partitions plus global context, all186 Airflow tasks successful, export47,379,950bytes across seven Parquet tables plus lineage/metrics metadata. Runtime8h52m. The finalizer succeeded and EC2 is stopped.
- Account498768607823, profile `gpu-fleet`, region `us-east-2`; worker `i-09dc0bfe45b0d275f`, m6a.2xlarge,60-GiB gp3. Temporary500-GiB volume deleted.
- Bucket `gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6`; live report `run-logs/full-silver-20261119/status.json`. Check freshness, final `verified_complete`, `run_outcome`, and service result before declaring success.
- Airflow3.0.6/LocalExecutor/PostgreSQL15; isolated production folder containing only the new Silver/Gold DAGs. Spark local[6],12-GiB heap. API binds worker loopback8080; local dashboard requires an SSM tunnel.
- `/opt/gpu-fleet/production.env.sh` contains private configuration. Legacy `env.sh` has pilot limits. Never publish secrets, Airflow database dumps or authentication files.
- Supervisor: minute health checks, two-hour checkpoints,10-GiB disk floor,30-hour internal cap,31-hour service cap,34-hour boot guard; independent180-second finalizer shutdown watchdog. No processing is currently active; do not restart EC2 for a local S3 download.

## Completion-checker recovery (November20)

After all380 Airflow tasks succeeded at2:19PM, ops/silver_supervisor.py failed its late project import and the guard stopped EC2. Fixed its script import path and preserved original wall timing. An isolated-interpreter test passed. Restarted the same worker under a30-minute recovery timer and reran only completion verification; all378 source/code/output manifests and192 audit bindings passed at4:04PM. Gold then launched. Gold’s independent16-hour guard is armed before the recovery timer is canceled; no Spark Silver rerun occurred.

## Recovery and semantics

Initial attempt failed on pod day5/hour06 physical schema drift. Full9,049-footer preflight and12 recovery tests passed; native-schema safe numeric widening and typed null restoration fixed it. The failing day and its join audit passed before R2. A finalizer dependency cycle was repaired and independent watchdog behavior tested.

Day5/hour06 has zero requests but positive GPU activity: preserve/flag, do not infer idle capacity. Current code SHA is recorded in saved Silver manifests. Old code/benchmark attempts remain historical. Read current accepted paths only, never wildcard versions.

## Next actions

1. Download and dashboard build were authorized November23 and completed locally. Do not overwrite the verified bundle with fixtures. Use examples/download_gold.py / verify_gold_bundle.py for reproducible setup.
2. Review dashboard locally; public deployment and a portable hosting/data-package strategy remain pending. Bundle is gitignored, so source-only deployment will not contain real data. Do not reintroduce AWS reads on visitor interactions.
3. Keep EC2 stopped and retain S3/root until verified independent backup and explicit teardown. The user has not authorized deleting these resources in this phase.
4. Latest published checkpoint before this work: ac56831 on gpu-fleet-analytics. Dashboard changes are local/uncommitted in this turn; do not imply they were pushed or main merged.

## Navigation

[Evidence](docs/evidence/README.md) · [Data contracts](docs/data/COLUMN_CONTRACTS.md) · [Silver runbook](docs/runbooks/SILVER_RUNBOOK.md) · [Gold plan](docs/design/GOLD_TRANSFORMATION_PLAN.md). Historical operations and local helper paths remain in [archived handoff](docs/archive/HANDOFF-history.md).

Repository was renamed from Data-Eng to gpu-fleet-analytics. Main CloudFormation still reflects historical t3.large/30-GiB defaults: do not overwrite approved live sizing blindly. Credits are shared/delayed; no remaining-balance guarantee.
