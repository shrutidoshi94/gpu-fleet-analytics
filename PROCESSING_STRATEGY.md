# Proposed processing strategy

Status: **discussion draft; processing is paused** as requested November 19, 2026. No full backfill or compute resize is authorized by this document.

## Verified starting point

All four archives have been downloaded and their Parquet members preserved in S3 Bronze. All source CRC and destination key/size inventories passed. Source archive hashes and verification evidence are saved locally and in S3. The temporary 500 GiB EBS volume `vol-028dceeb16c897cfc` was unmounted and deleted at explicit user request; stack `gpu-fleet-data` is deleted. The original worker and 30 GiB system disk remain. Worker is confirmed stopped for strategy review. Keep it stopped and processing paused until the user approves resuming.

S3-only pilots passed for network day109 (766207 observations), server day109 (775104 server-hours), workload day109 (2816548 GPU-requesting pod-hours), and the complete execution summary (40522321 records). Independent PyArrow checks compared Bronze aggregates and actual S3 Silver row counts with Gold. All four saved reports rendered in the offline dashboard.

A subsequent workload rerun check was canceled when the user requested pause (SSM command `b7a969f7-5d90-4b26-b059-5563524624fe`, Cancelled/137). Treat day109 workload outputs as needing a fresh successful rerun before consuming new results. Its test probes may remain in S3; do not treat them as source data. The canceled Airflow run may still require state cleanup when a persistent scheduler is configured.

## Understand Bronze: real sample rows and insight opportunities

This section was added from **read-only S3 inspection on November 19, 2026**. EC2 remained stopped. No Spark/Airflow processing or layer rewrite was performed for this review.

“Top 10” here means **the first ten physical rows in one selected Parquet file**, not the ten largest values. There is no inherent global ordering in a Parquet dataset. Daily tables use the aligned **day 109, hour 00** partition; execution uses its global file. The rows are examples, not random samples or whole-trace statistics. Extra GPU-requesting workload examples are included because the first ten workload rows happen to be CPU-only.

For readability, hashed IDs are shown as stable aliases (P=pod, S=server, W=workload, C=cluster, A=access-switch group). Aliases only change display; full original IDs and unrounded numbers are retained in the linked CSV/JSON files. See [alias mapping](docs/bronze_samples/id_aliases.csv). `NULL` is missing data, not zero. CSV blanks represent nulls; JSON is authoritative for original value types and nulls. Independently sampled tables need not share any of these ten IDs: join on the complete keys, never row position.

### 1. Workload table: what was observed on each server

Grain: one observed pod/server/hour. A repeated pod across hours represents repeated observations, not new jobs. The selected hour contains **1,489,135 source records**; the ten rows below are only its opening records.

Fields form several useful groups: identity and placement; public workload/model/priority labels; requested resources; measured resource activity; and readiness/scheduling context. [Official field definitions](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-gpu-v2026/docs/schema.md) distinguish GPU requests, attributed GPU-hours, and SM measurements.

Source object: `bronze/pod/day=109/hour=00/part-000.parquet`.

| Row | `pod_id` | `server_id` | `gpu_spec_public` | `gpu_request` | `avg_gpu_sm_util` | `avg_cpu_request_util` | `job_type_public` | `ready_delay_sec` |
|---|---|---|---|---|---|---|---|---|
| 1 | P001 | S001 | A10 | 0 | NULL | 1.19258 | unknown | 4 |
| 2 | P002 | S002 | XPU-A | 0 | NULL | 0.499787 | unknown | 6 |
| 3 | P003 | S003 | XPU-A | 0 | NULL | 2.37084 | unknown | 6 |
| 4 | P004 | S004 | XPU-B | 0 | NULL | 1.74808 | unknown | -1 |
| 5 | P005 | S005 | A10 | 0 | NULL | 0.608439 | unknown | -1 |
| 6 | P006 | S006 | A10 | 0 | NULL | 0.127335 | unknown | 4 |
| 7 | P007 | S007 | XPU-B | 0 | NULL | 0.0394137 | unknown | 9 |
| 8 | P008 | S008 | XPU-B | 0 | NULL | 0.775807 | unknown | -1 |
| 9 | P009 | S009 | L20 | 0 | NULL | 0.05 | unknown | 185 |
| 10 | P010 | S010 | XPU-A | 0 | NULL | 0.462279 | unknown | 90 |

[All ten rows and all columns: CSV](docs/bronze_samples/pod_10rows.csv) · [JSON, schema, and source provenance](docs/bronze_samples/pod_10rows.json)

**What these actual rows reveal:** all ten request zero GPUs and have null SM measurements, even though their hosts have GPUs. CPU-only co-tenants must not be counted as GPU-consuming workloads. Several CPU/request utilization values exceed 1; that need not be invalid. All ten have unknown workload labels, so dropping unknown categories would silently change the population. These observations do **not** establish their prevalence across the trace.

**Additional GPU examples from the same source hour:** first ten encountered with `gpu_request > 0` while scanning 4,096 rows. This is a filtered example set, not a representative sample or top-utilization ranking.

| Row | `pod_id` | `gpu_spec_public` | `job_type_public` | `priority_class` | `gpu_request` | `used_gpu_hours` | `avg_gpu_sm_util` |
|---|---|---|---|---|---|---|---|
| 1 | P021 | XPU-E | online_inference | HP | 1 | 1 | 4.96857 |
| 2 | P022 | XPU-E | online_inference | HP | 1 | 1 | 52.3876 |
| 3 | P023 | XPU-E | online_inference | HP | 1 | 1 | 0 |
| 4 | P024 | XPU-A | online_inference | HP | 1 | 1 | NULL |
| 5 | P025 | XPU-A | offline_inference | LP | 1 | 0.272222 | NULL |
| 6 | P026 | L20 | online_inference | HP | 1 | 1 | 8.48309 |
| 7 | P027 | XPU-A | offline_inference | LP | 1 | 0.0833333 | NULL |
| 8 | P028 | XPU-E | offline_inference | LP | 1 | 1 | 40.6083 |
| 9 | P029 | L20 | offline_inference | LP | 1 | 1 | 78.7585 |
| 10 | P030 | A100 | online_inference | HP | 4 | 4 | 0.728 |

[Full GPU example rows: CSV](docs/bronze_samples/pod_gpu_10rows.csv) · [JSON with provenance](docs/bronze_samples/pod_gpu_10rows.json)

These examples separate **missing SM**, **zero measured SM**, and **positive measured SM**. A four-GPU request with 4 attributed GPU-hours has an SM reading around 0.728, while another pod reports about 78.76. These are investigation candidates, not proof of waste. `used_gpu_hours` and SM activity describe different things. Partial-hour GPU attribution also appears (for example 0.08333 GPU-hours). `state_public=Unknown` occurs alongside real GPU activity and must not be equated with inactivity.

**Useful analyses:** workload and model mix; resource request/usage comparisons; CPU co-tenancy; GPU-memory patterns; telemetry coverage; priority/readiness relationships; and persistent low-activity cohorts. Confirm units before converting `gpu_mem_request`, `avg_memory_util`, or SM readings into percentages or ratios.

<details>
<summary>Actual columns and types in this Bronze Parquet file</summary>

| Column | Actual Parquet type |
|---|---|
| `pod_id` | `string` |
| `workload_id` | `string` |
| `server_id` | `string` |
| `cluster_id` | `string` |
| `state_public` | `string` |
| `gpu_spec_public` | `string` |
| `server_gpu_count` | `int64` |
| `server_cpu_capacity_cores` | `int64` |
| `priority_class` | `string` |
| `job_type_public` | `string` |
| `model_type_public` | `string` |
| `is_genai_request` | `bool` |
| `gpu_request` | `double` |
| `gpu_mem_request` | `int64` |
| `cpu_request_cores` | `double` |
| `used_gpu_hours` | `double` |
| `avg_gpu_sm_util` | `double` |
| `avg_gpu_mem_gib` | `double` |
| `avg_cpu_request_util` | `double` |
| `avg_memory_util` | `double` |
| `ready_status` | `bool` |
| `schedule_delay_sec` | `int64` |
| `ready_delay_sec` | `int64` |
| `day`, `hour` | Derived from S3 partition path, not stored columns |

All physical fields in these sampled files are nullable in the Parquet schema. This does not mean every field is actually missing. File schema does not establish whole-trace null rates.

</details>

### 2. Server inventory: the capacity denominator

Grain: one server/hour. The selected hour contains **32,296 server records**. These are inventory observations, not workload demand. Count distinct servers for machine counts; summing repeated hourly rows produces server-hours.

Source object: `bronze/server/day=109/hour=00/part-000.parquet`.

| Row | `server_id` | `cluster_id` | `asw_id` | `gpu_spec_public` | `gpu_count` | `cpu_capacity_cores` |
|---|---|---|---|---|---|---|
| 1 | S011 | C006 | A001 | XPU-B | 1 | 192 |
| 2 | S012 | C006 | A002 | A10 | 1 | 128 |
| 3 | S013 | C007 | A003 | XPU-E | 16 | 192 |
| 4 | S014 | C001 | A004 | A10 | 1 | 128 |
| 5 | S015 | C006 | A005 | L20 | 2 | 192 |
| 6 | S016 | C003 | A006 | A10 | 1 | 128 |
| 7 | S017 | C007 | A007 | XPU-E | 16 | 192 |
| 8 | S018 | C008 | A008 | XPU-A | 8 | 192 |
| 9 | S019 | C004 | A009 | L20 | 2 | 192 |
| 10 | S020 | C001 | A010 | A10 | 1 | 128 |

[All ten rows and all columns: CSV](docs/bronze_samples/server_10rows.csv) · [JSON, schema, and source provenance](docs/bronze_samples/server_10rows.json)

**What these actual rows reveal:** hosts have 1, 2, 8, or 16 GPUs and different CPU capacities. A server count alone is not fleet GPU capacity. Public XPU labels are anonymized categories; do not guess their vendor/model or equate GPU counts with equal computational performance.

**Useful analyses:** hourly GPU capacity by model/cluster; server-size mix; capacity changes; access-switch grouping; workload density and request headroom after a safe join. Never join raw pod rows onto inventory and then sum repeated capacity. Aggregate pods to server/day/hour first, then join once to inventory; retain servers with no matched workload observations and report unmatched workload keys.

<details>
<summary>Actual columns and types in this Bronze Parquet file</summary>

| Column | Actual Parquet type |
|---|---|
| `server_id` | `string` |
| `cluster_id` | `string` |
| `asw_id` | `string` |
| `gpu_spec_public` | `string` |
| `gpu_count` | `int64` |
| `cpu_capacity_cores` | `int64` |
| `day`, `hour` | Derived from S3 partition path, not stored columns |

All physical fields in these sampled files are nullable in the Parquet schema. This does not mean every field is actually missing. File schema does not establish whole-trace null rates.

</details>

### 3. Network table: hourly receive/transmit activity

Grain: one observed server/hour. The selected hour contains **31,917 traffic records**. The source names end in `gibps`, but their documented unit is **GiB/s (bytes), not gigabits/s**. Rates are hourly averages, so this table cannot reveal second-level bursts.

Source object: `bronze/network/day=109/hour=00/part-000.parquet`.

| Row | `server_id` | `rx_gibps_avg` | `tx_gibps_avg` |
|---|---|---|---|
| 1 | S021 | 1.21013 | 1.0352 |
| 2 | S022 | 2.67528e-05 | 3.78746e-05 |
| 3 | S023 | 0.477814 | 0.290698 |
| 4 | S024 | 0.86263 | 0.00223774 |
| 5 | S025 | 1.09995 | 1.05911 |
| 6 | S026 | 1.74923 | 1.69754 |
| 7 | S027 | 1.17779 | 0.961286 |
| 8 | S028 | 1.0751e-05 | 1.98514e-05 |
| 9 | S029 | 0.00823724 | 0.000332795 |
| 10 | S030 | 0.887423 | 0.854766 |

[All ten rows and all columns: CSV](docs/bronze_samples/network_10rows.csv) · [JSON, schema, and source provenance](docs/bronze_samples/network_10rows.json)

**What these actual rows reveal:** hourly receive rates span roughly 0.00001075 to 1.749 GiB/s in these ten rows, with some strong receive/transmit asymmetry. A fleet mean alone can hide hot servers. The traffic and inventory files have different row counts; row-count differences alone do not prove which server IDs overlap.

**Useful analyses:** median/p95/p99 server traffic; hottest servers and concentration; receive/transmit balance; and associations with workload composition during days 109–115. Measure join coverage explicitly. Do not claim a network bottleneck or link utilization without a released link-speed denominator, or causality from correlation. No traffic outside the released window means missing data, not zero traffic.

<details>
<summary>Actual columns and types in this Bronze Parquet file</summary>

| Column | Actual Parquet type |
|---|---|
| `server_id` | `string` |
| `rx_gibps_avg` | `double` |
| `tx_gibps_avg` | `double` |
| `day`, `hour` | Derived from S3 partition path, not stored columns |

All physical fields in these sampled files are nullable in the Parquet schema. This does not mean every field is actually missing. File schema does not establish whole-trace null rates.

</details>

### 4. Execution summary: observed execution spans

Grain: one source pod/workload execution-span record. This file contains **40,522,321 records** and has **no day/hour partition**. These ten records are not sorted by duration. A long-lived inference service is different from a batch job, so duration comparisons should be segmented by workload type.

Source object: `bronze/execution/all/part-000.parquet`.

| Row | `pod_id` | `job_type_public` | `gpu_spec_public` | `priority_class` | `gpu_request` | `duration_hours` | `ready_delay_sec` | `ready_status` |
|---|---|---|---|---|---|---|---|---|
| 1 | P011 | online_inference | A10 | HP | 0.24 | 1437.98 | 24 | true |
| 2 | P012 | offline_inference | XPU-E | LP | 1 | 0.661111 | -1 | false |
| 3 | P013 | online_inference | L20 | HP | 1 | 408 | 23 | true |
| 4 | P014 | online_inference | XPU-E | HP | 1 | 764.356 | -1 | true |
| 5 | P015 | online_inference | A10 | HP | 0.31 | 0.288889 | -1 | false |
| 6 | P016 | offline_inference | XPU-E | LP | 1 | 1.36667 | 5 | true |
| 7 | P017 | online_inference | XPU-E | HP | 1 | 1223.96 | 7 | true |
| 8 | P018 | online_inference | XPU-B | HP | 1 | 1631.97 | -1 | true |
| 9 | P019 | online_inference | XPU-E | HP | 1 | 545.994 | -1 | true |
| 10 | P020 | offline_inference | XPU-A | LP | 1 | 0.827778 | -1 | true |

[All ten rows and all columns: CSV](docs/bronze_samples/execution_10rows.csv) · [JSON, schema, and source provenance](docs/bronze_samples/execution_10rows.json)

**What these actual rows reveal:** GPU requests include fractional values such as 0.24 and 0.31. One span is about 1,438 hours (roughly 60 days), while another is about 0.66 hours. Several readiness delays are `-1`, including rows with `ready_status=true`; negative delay must not automatically mean readiness failure. Preserve the raw value and flag it as a sentinel candidate until upstream semantics are confirmed. Missing `workload_id` is present; do not invent identities or blindly join missing values.

**Useful analyses:** duration distributions and long tails by workload/model/priority; readiness and scheduling delays; fractional-GPU request patterns; missing metadata. Avoid reporting a mean alone. Observed execution spans are not automatically completed-job runtimes or causal performance scores. Do not allocate a global span to daily dates without a defensible time mapping.

<details>
<summary>Actual columns and types in this Bronze Parquet file</summary>

| Column | Actual Parquet type |
|---|---|
| `pod_id` | `string` |
| `workload_id` | `string` |
| `server_id` | `string` |
| `gpu_spec_public` | `string` |
| `priority_class` | `string` |
| `job_type_public` | `string` |
| `model_type_public` | `string` |
| `is_genai_request` | `bool` |
| `gpu_request` | `double` |
| `duration_hours` | `double` |
| `schedule_delay_sec` | `int64` |
| `ready_delay_sec` | `int64` |
| `ready_status` | `bool` |
| `schedule_status` | `bool` |

All physical fields in these sampled files are nullable in the Parquet schema. This does not mean every field is actually missing. File schema does not establish whole-trace null rates.

</details>

### Insight-first Silver and Gold design — proposed, not executed

The current prototype is deliberately narrow: workload Silver keeps only six fields and filters out zero-GPU requests. Its Gold mainly reports means. **That is insufficient for the broader questions below.** Before full processing, expand the design rather than processing every day into the same limited summaries.

Recommended Silver structure:

- Preserve all useful workload fields and observed dimensions with types/units documented; keep CPU-only records in the cleaned base table and derive a GPU-requesting view for GPU analyses. This costs more storage than the current narrow projection, so measure compressed output on one day first. A logical view need not duplicate physical data.
- Preserve observed versus derived values separately. Add `is_gpu_requesting`, measurement-presence flags, sentinel/missing-value flags, and a quality summary. Do not turn null utilization into zero or unknown state into failed state.
- Check candidate keys without silently deduplicating legitimate observations. Inventory key: server/day/hour; workload candidate: pod/server/day/hour. Repeated pods in different hours and multiple pods per workload are expected. Execution summary needs its own grain checks.
- Keep clean inventory, workload, network, and execution facts separate. Build derived server-hour workload aggregates before capacity joins. Track unmatched keys and join row counts.
- Standardize partition/time types and retain source lineage. Preserve original values for flagged records or quarantine them with reasons. Do not clamp normalized CPU utilization at 1 or assume undocumented units.

| Dashboard question | Source fields / tables | Proposed Gold output | Current prototype gap / limit |
|---|---|---|---|
| How does capacity change by GPU model and cluster? | Inventory: GPU count/model, cluster, hour | Hourly capacity and distinct-server trends | Current GPU-model summary lacks hourly/cluster detail |
| Which workloads account for GPU demand and attributed time? | Workloads: type, model, request, used GPU-hours | Request observations and attributed GPU-hours by cohort | Current workload Silver drops most dimensions and GPU-hour attribution |
| Which cohorts show persistently low measured activity? | Workloads: SM, model, type, priority, time | Coverage-aware distributions, percentiles, persistent cohorts | Current mean hides tails and missing measurements; not proof of waste |
| How much telemetry is missing? | Nulls, unknown labels, readiness and sentinel flags | Coverage and quality trends alongside every metric | No dedicated quality Gold table yet |
| Are CPU-only co-tenants significant on GPU servers? | GPU request=0, CPU request/utilization, memory, placement | Co-tenancy counts and CPU-demand observations | Current early GPU filter removes them from Silver |
| Do priority groups differ in readiness or delay? | Priority, readiness flags, valid delay fields | Median/p95 delays and missing-value rates by cohort | Preserve negative markers; avoid causal claims |
| Which servers have request headroom? | Server-hour workload aggregates + unique inventory | Request-to-capacity ratios, density, unmatched keys | Safe aggregation/join not implemented; requests are not physical utilization |
| Are GPU-memory patterns associated with low SM activity? | SM, average GPU memory GiB, model | Joint distributions by model/workload | Units of memory-request field must be verified before request/use ratios |
| Is network traffic concentrated? | Network RX/TX, server, hour | p50/p95/p99, top-server contribution, directional balance | Current network Gold has only hourly means |
| How does traffic align with workloads? | Network + server-hour workload aggregates | Seven-day comparison with join coverage | No cross-table analysis yet; no causal/bottleneck claim |
| How do execution spans differ across cohorts? | Duration, type, model, priority, readiness | CDF/percentiles and long-tail summaries | Current duration grouping is only workload type; approximate percentiles |
| Are large workloads made of many pods? | Workload ID, pod ID, resource requests, time | Pod counts per known workload and demand concentration | Missing workload IDs require explicit coverage; never count null as one workload |

Start with days **109–115**, where workload, inventory, and network data overlap, plus the separate global execution summary. Compare cohort coverage, distributions, and join correctness before the full 185-day backfill. These are proposed analyses supported by available columns, not findings already established.

### Important limits on what this trace can tell us

- No raw names, actual calendar dates, user/customer identities, or external geography are provided.
- Hourly observations cannot reconstruct second-level spikes or exact device placement.
- Capacity, requested GPUs, attributed GPU-hours, and measured SM activity are different metrics. Label each denominator and weighting rule.
- Compare hardware/workload cohorts before pooling measurements. Different public GPU buckets are not equivalent compute throughput.
- Ten-row examples explain records; they cannot establish whole-trace distributions, data quality rates, or causal conclusions.
- Exact device-level fragmentation, energy/carbon consumption, cost savings, and causal scheduling improvements need additional assumptions or data. Keep any server-level proxies explicitly labeled.

The next processing strategy should prioritize these questions and decide which Silver fields/Gold products are required **before** a full run. No implementation change or resumed processing is authorized by this review.

## Scope and outputs

- Keep Bronze immutable during transformation.
- Process workload and server days 0–184, network days 109–115, and execution summary once globally.
- Preserve day boundaries so failures/retries replace only the selected dataset/day.
- Start with an aligned seven-day window (109–115) to validate the combined dashboard, then process the remaining days.
- Keep missing network days absent/null rather than manufacturing zero traffic.
- Keep saved Gold locally throughout; verify it remains sufficient for the dashboard after AWS teardown.

## Compute options and decision

Ohio on-demand compute prices verified through AWS Pricing API November 19, 2026:

| Worker | CPU / RAM | Compute per hour | Tradeoff |
|---|---|---:|---|
| Existing t3.large | 2 vCPU / 8 GiB | $0.0832 | Lowest hourly cost; Standard CPU credits can throttle sustained workloads |
| m6i.xlarge | 4 vCPU / 16 GiB | $0.192 | More stable CPU capacity and memory |
| m6i.2xlarge | 8 vCPU / 32 GiB | $0.384 | Best simple candidate for faster processing; needs benchmark to establish value |

Add public IPv4 ($0.005/h), system disk, and S3 usage. Credit applicability and balance remain unverified.

Measured current-worker pilots: workload day109 took 99.65 seconds including Airflow orchestration; server day109 took about 85 seconds; the global execution report took about ten minutes. Extrapolation suggests about ten hours for the full serial run before credit throttling, day variation, retries, or repeated startup costs. The earlier 20–50h estimate was conservative and preceded workload measurements. Do not promise a proportional speedup from extra CPUs.

Recommendation for speed: one nonburstable 8-CPU worker, first running a single Spark application with suitable parallelism/memory. Benchmark the same day before committing to the long run. Recommendation for minimum cost: keep the existing worker and measure sustained throughput/CPU credits over the seven-day window.

Resizing the current worker requires a brief stop/start. Archive/ingestion jobs are finished, so a controlled resize can be planned, but it has not been approved or performed. Before any restart for a long backfill, address the normal two-hour boot stop timer (it is currently inactive, but remains enabled for future boots). Match infrastructure code to any approved change.

## Parallelism

The current pipeline uses local Spark and the backfill DAG has max_active_tasks=1. The `dags test` runner executes locally and is not a distributed scheduler.

1. Begin with one Spark job at a time; increase its CPU/memory configuration to match the chosen worker.
2. If a representative benchmark shows startup/I/O idle time dominates, consider two independent day jobs sharing the larger worker, assigning a bounded number of cores and memory to each.
3. True scheduled parallel jobs require a properly configured Airflow executor and metadata database; use PostgreSQL rather than relying on the current local SQLite demonstration setup. Do not assume that changing max_active_tasks alone makes `dags test` a parallel production scheduler.
4. Multiple EC2 workers require additional execution/coordination infrastructure. Use them only if the single-worker benchmark fails the agreed completion target.

Never overlap two writers for the same dataset/day prefix. Different day/dataset prefixes may be processed independently, subject to memory/disk limits.

## Execution gates after strategy approval

1. Inspect canceled workload rerun state; rerun day109 from S3, verify counts/means, stale-file removal, and neighboring-day preservation.
2. Confirm the chosen worker's Spark configuration and system-disk headroom. Benchmark the same input and record elapsed time, CPU/credit balance, peak memory, scratch usage, and failures.
3. Run the aligned 109–115 window and validate every Gold report's coverage/counts. Check that any future workload/server join uses server ID, day, and hour, preserves row counts, and reports unmatched records.
4. Re-estimate the remaining full backfill from observed sustained throughput. Keep a resumable per-dataset/day run ledger or use persistent Airflow run state so restarts do not require blindly recomputing everything.
5. Process remaining days, verify expected partition coverage, and save all Gold summaries locally.
6. Check dashboard views and metric definitions, then stop/terminate compute when approved/appropriate. Retain or remove S3 layers according to the agreed reproduction/retention plan.

## Metric interpretation

Mean SM utilization is an unweighted mean across measured GPU-requesting pod-hours in source units; it is not whole-fleet utilization. Inventory GPU-hours describe observed capacity. Request-to-capacity ratios, GPU-time attribution, and measured SM utilization must be labeled separately if added. Workload/server joins must avoid duplicating source rows. The execution summary is global and should not be presented as daily telemetry.

## Decisions to settle before execution

- Lowest-cost existing worker, or a benchmark on a 4/8-CPU worker?
- Is one-worker serial processing sufficient for the completion target, or is parallel Airflow execution required?
- Which dashboard questions should the seven-day window validate before the complete trace is processed?

The user's latest instruction is to agree on strategy before starting processing. Preserve that pause even if an earlier resource-choice prompt is answered later; a resource preference alone is not permission to start the backfill.
