# Bronze → Silver

The completed run processed 378 partitions: 185 server days, 185 pod days, seven network days and one global execution table. It retained 6,759,480,015 accepted rows and completed 192 inventory audits. See [execution evidence](../evidence/README.md).

## Transformations

`pipelines/silver.py` reads every physical Parquet schema before Spark ingestion, groups compatible schemas, safely widens numeric values and restores typed nulls. Integers outside the exact double range fail conversion. Original fields remain; quality flags record unknown categories, negative values, NaN/infinity and the known anomalous source hour.

Inventory and network rows with missing server keys or duplicate server-hour keys are quarantined. Repeated pod/execution candidate keys remain in Silver because their uniqueness is unproven. CPU-only and fractional GPU requests are retained. See [column contracts](../data/COLUMN_CONTRACTS.md) and the [Bronze profile](../data/BRONZE_PROFILE.md).

Each partition checks input = accepted + quarantined, Spark read-back counts and independent Arrow footer counts. Inventory joins use semi/anti joins so repeated workload observations cannot multiply server capacity.

## Run

Use Python 3.11 and Java 17. Install `app/requirements.txt` and `pipelines/requirements.txt`; install Airflow with the official constraints shown in [CI](../../.github/workflows/sample.yml). The [infrastructure guide](../../infra/README.md) provides worker setup.

```bash
python -m examples.make_silver_sample /tmp/gpu-silver-sample
export TRACE_PROJECT="$PWD" TRACE_SILVER_ROOT=/tmp/gpu-silver-sample
export AIRFLOW_HOME=/tmp/gpu-silver-airflow
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" SPARK_LOCAL_IP=127.0.0.1
airflow db migrate
airflow dags test gpu_silver_backfill 2026-11-19 \
  -c '{"datasets":["server","pod","network","execution"],"days":[109]}'
```

For AWS, source the worker environment and trigger `gpu_silver_backfill` with a new run ID. Select the datasets and days explicitly; execution is global and network only covers 109–115. The DAG runs one transformation at a time, retries once, then performs final inventory audits. Running a sample DAG test does not constitute a full production backfill.

## Publication and recovery

Attempts are immutable ZSTD Parquet under `silver_versions/`. The partition's `manifests/silver/...current.json` pointer is written last. Consumers read exactly the accepted path in that pointer. They must not wildcard all attempts.

Unchanged source/code/output inventories are reused. A forced rerun writes a new attempt and replaces only the current pointer after validation. An interrupted attempt leaves the preceding published partition readable. Concurrent writers to the same partition are unsupported. Final audits bind both the accepted workload attempt and the inventory source fingerprint; repeat them if either changes.

Unreferenced attempts require manual retention cleanup. Remote inventory checks compare keys, sizes and ETags; they are not cryptographic rehashes of every S3 byte.

## Engineering decisions

- Physical schema drift in day 5 required native-type reads before normalization; the initial strict reader failed safely.
- Disk spilling on the largest benchmark day triggered the free-space guard; expanding the root from 30 to 60 GiB allowed completion.
- Six threads processed the measured pod-day case in 277.4 seconds versus 363.3 seconds with three threads on the same host. This did not measure multi-machine scaling.
- One worker was sufficient for the bounded trace backfill. [Benchmarks](../evidence/AWS_BENCHMARK_RESULTS.md) retain the timings and cost assumptions.

Historical run events and manifests remain in the evidence directory. Current code is refactored; the exact deployed source is retained separately for hash verification. A current-code rerun must publish manifests with its new code identity.
