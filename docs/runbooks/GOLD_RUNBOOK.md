# Gold runbook

Authorized November20,2026. Release/run ID: `gold_20261120`. Source: pinned production Silver; no new archive downloads. Silver verified at4:04PM Pacific after a completion-checker import fix. The automatic handoff launched Gold; Gold completed successfully November21 at12:56AM Pacific, with an exported47.4-MB bundle; EC2 is stopped. Local download and integrity/consolidation validation were completed November23; see DASHBOARD.md for the offline app. No Silver transformations were repeated.

## Readiness and orchestration

`gold.py pin` requires `verified_complete:true` and successful Silver DAG state in the production S3 status. It pins378 current Silver manifests and the exact source/code identities. Production cannot use the local synthetic bypass.

The existing Silver service finalizer has an authorized success-only handoff: queue Gold, preserve Silver evidence, and return without stopping the worker. On Silver failure its original shutdown path remains. Gold explicitly waits for Silver finalization to end before triggering `gpu_gold_backfill`; systemd After= alone was found insufficient and is not relied upon. A harmless predecessor/successor probe passed after that correction.

The Gold DAG runs serially: pin -> days3/5/109/173 pilot -> budget gate ->181 remaining day tasks -> global execution -> release validation -> offline export. A pilot projected daily runtime above12hours stops the run for review. No second EC2 instance is used. Read run-scoped `queued.json` before startup and `status.json` after startup under `run-logs/gold/gold_20261120/`.

## Outputs and interpretation

Daily products: capacity_hourly, activity_evidence_hourly, workload_mix_hourly, placement_daily, planning_quality; network_context_hourly only109–115. Global execution_context_global has no invented daily mapping. Each Gold manifest references immutable ZSTD Parquet outputs and the pinned Silver plan. Publish the release manifest only after all185 daily units and global context validate; consumers never wildcard attempts.

Capacity is reconciled independently against unique inventory. Missing model identity is distinct from an unmatched server key. Missing telemetry is not a cohort mismatch; unknown capacity/ratios stay null. Anomalous source hours and duplicate candidates do not produce usable headroom. Pinned Silver object inventories are rechecked before Gold publication. Unknown observations remain unknown; unmatched/mismatched cohorts are visible. Request sums are partial observational evidence. All procurement decision_ready flags remain false until lifecycle/completeness semantics are reviewed. Source anomalies and candidate duplicates remain explicit. No0–100SM clipping or automatic reclaim quantity is produced.

The initial release is the compact factual foundation. Ranked opportunities, scenario UI, additional histograms and expanded distribution metrics are not yet claimed implemented. Full hourly series and numerator/count components allow many window-level dashboard calculations without rereading Silver.

## Guards and recovery

Reuse the tested Silver subprocess guard: one-hour child limit,10-GiB disk floor, process-group cleanup on timeout/signals. Airflow day timeout75minutes, retry once. Supervisor polls health each minute and checkpoints PostgreSQL every two hours. Internal limit15hours, service/independent timer16hours; finalizer has an independent180-second shutdown watchdog. A five-minute handoff watchdog covers a failed successor start. Finalizer saves evidence and stops compute on completion/failure.

`ops/gold_task.py` reuses a matching unit only when its plan/code and actual output inventories agree. A failed attempt cannot replace a prior pointer. Export retries verify existing bundle identity/hashes before reusing/uploading it. Do not automatically restart a failed Airflow run under the same ID: inspect failure and pinned release first.

## Manual commands

On the configured worker, source private `production.env.sh` and keep the tested Spark six-thread/12-GiB settings. These are individual reproduction commands; production orchestration uses the guarded DAG.

```bash
python gold.py pin --root s3://YOUR_BUCKET --release YOUR_RELEASE
python gold.py day --root s3://YOUR_BUCKET --release YOUR_RELEASE --day 109
python gold.py context --root s3://YOUR_BUCKET --release YOUR_RELEASE
# Run every pinned day before publication.
python gold.py publish --root s3://YOUR_BUCKET --release YOUR_RELEASE
python ops/export_gold.py --root s3://YOUR_BUCKET --release YOUR_RELEASE --destination /path/to/offline-bundle
```

## Offline export and teardown gate

Export combines compact daily outputs into local Parquet files with bundle.json, lineage.json and METRICS.md. It verifies file hashes and uploads one portable copy under `gold_exports/gold_20261120/`. Export size is measured, not assumed to meet the100-MiB target. The dashboard must read the downloaded bundle without AWS clients or credentials.

After the run, download and independently verify hashes and metric samples; implement/test the final capacity dashboard offline. Preserve Gold/export/Silver completion evidence in the repo and update evidence checksums. Do not delete AWS data until verified local backups and the user's teardown instruction.
