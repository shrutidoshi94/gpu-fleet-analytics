# Proposed processing strategy

## Current operation — authorized Silver recovery running

Updated November 19, 2026, 7:57 PM Pacific. The user authorized the full Bronze-to-Silver transformation. Airflow run `silver_full_20261119_r2` started at 7:50:54 PM Pacific on the existing m6a.2xlarge (8 vCPU / 32 GiB), with the approved 60-GiB gp3 root, Spark local[6] and 12-GiB driver heap. Latest snapshot: 3/378 transformation partitions succeeded, one running, no failed tasks, 52.97 GiB disk free. This is a dated snapshot; read live status before taking action.

The initial full attempt failed on pod day5/hour06 because one source file uses integer numeric fields and null-only utilization fields. A full audit of all 9,049 Bronze footers is now complete. The reader groups native physical schemas and permits only safe widening, rejecting unsafe integer-to-double values beyond ±2^53. Added null/blank/Unknown/non-finite/boolean counts and partition-scoped source observations. Day5/hour06 has zero requests but some positive GPU activity: preserve and flag it; do not interpret it as proven idle capacity. See [BRONZE_PROFILE.md](../data/BRONZE_PROFILE.md) and the 46-field [COLUMN_CONTRACTS.md](../data/COLUMN_CONTRACTS.md). Footer coverage does not prove every row-level semantic invariant.

All 12 local recovery regression tests passed. The actual failing pod day5 (27,805,129 rows), inventory day5, and final join audit passed under the production Airflow scheduler before R2 started. Prior immutable attempts remain available; changed code/source identity causes reprocessing, not blind reuse. Consumers must resolve current manifests, never wildcard versions. Gold is not part of this run.

PostgreSQL15 and Airflow3.0.6 LocalExecutor are configured and validated in isolated production paths. Only the Silver DAG is loaded. API/scheduler/DAG processor are active; API binds 127.0.0.1:8080 and is available locally through an SSM tunnel. No inbound security-group port was opened. Legacy SQLite/pilot DAG history is preserved and inactive.

Worker health is checked every minute; comprehensive health checks and PostgreSQL checkpoints are saved every two hours. The supervisor fails on permanent task failure, stopped orchestration services, disk below10 GiB, or a30-hour runtime limit. Success requires all378 current partition manifests and192 final inventory audit bindings to verify. The service has a31-hour cap and boot guard34hours. Failure/completion finalization preserves evidence and shuts down EC2. The first attempt's finalizer timed out because of an Airflow service dependency cycle; that dependency was removed, shutdown is nonblocking, and a separate180-second shutdown watchdog was verified to survive finalizer timeout. Do not restart the old run or change these guards casually.

Live evidence: `s3://gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6/run-logs/full-silver-20261119/status.json`, `health-checks.jsonl`, task reports and metadata checkpoints in the same prefix. Check status freshness, `run_outcome`, `verified_complete`, and `service-result.json`; a historical DAG state alone is not sufficient. Worker: `i-09dc0bfe45b0d275f`, account498768607823, profile `gpu-fleet`, region `us-east-2`. Do not stop a healthy authorized run. No second worker or500-GiB disk is needed.

The pre-repair benchmark forecast was16–25hours and ~$7–$11 before credits including short retention. Additional quality checks/reprocessing can change that estimate; it is not a completion guarantee. AWS credit balance is delayed/shared. New capacity-planner Gold and offline export/dashboard require a subsequent implementation/review phase.

## Earlier design and benchmark checkpoint (superseded execution state)

The current operation above overrides older paused/unapproved/setup-pending statements below. Earlier measurements and proposed Gold designs remain historical evidence.

## Verified benchmark complete — worker stopped

All 9 benchmark cases and 4 final join audits completed. The existing worker is **stopped** as m6a.2xlarge (8 vCPU / 32 GiB) with the user-approved 60-GiB gp3 root (3,000 IOPS / 125 MiB/s). The tested single-worker Bronze-to-Silver scenario is **16–25 hours**, approximately **$7–$11 before credits** including short storage retention; Gold and production scheduler setup are excluded. Eight unique Silver partitions are committed, 370 remain. Recommend the tested single worker; no parallel-machine speedup is claimed. Network109 has 190 retained unmatched inventory observations. See [AWS_BENCHMARK_RESULTS.md](../evidence/AWS_BENCHMARK_RESULTS.md) for measurements, cost assumptions, coverage and alternatives. Full backfill remains unapproved; older status sections below are historical.


## Silver implementation and validation

`silver.py` and `gpu_silver_backfill` implement the full-width Silver-only path: CPU-only workloads are retained, source schemas/inventories and row accounting are checked, quality/quarantine data is recorded, and immutable attempts are exposed through current-version manifests. The real-data benchmark and final audits passed. See [SILVER_RUNBOOK.md](../runbooks/SILVER_RUNBOOK.md) and [AWS_BENCHMARK_RESULTS.md](../evidence/AWS_BENCHMARK_RESULTS.md). Legacy `pipeline.py` still creates narrow pilot Silver and Gold together; it is not the new Silver entry point. The capacity-planner Gold layer/dashboard and persistent production Airflow/PostgreSQL setup are not implemented yet. The full backfill awaits the user's topology decision.

Credit evidence: the user supplied management-account screenshots showing $153.53 estimated remaining and all four credit service lists covering EC2/S3/VPC/data transfer, and confirmed sharing activated for the project account. This is shared, delayed balance evidence, not a live guarantee. Prices were refreshed through AWS Pricing API on November 19, 2026.



Status: **benchmark completed; full backfill awaits topology approval**. The user authorized the completed worker resize, 60-GiB root expansion and benchmark only. Proposed Gold metrics below still require implementation/review.

## Dashboard audience and decisions: capacity planners only

The user has narrowed the dashboard to infrastructure capacity planners deciding whether to buy capacity and how to improve the efficiency of existing infrastructure. This is the binding product scope. The raw-data examples remain useful background; they do not imply a general-purpose telemetry explorer.

### Decision contract

Every primary view must support one of these decisions:

| Planner decision | Evidence to show | Action the evidence can support |
|---|---|---|
| Do we need more capacity, and in which GPU cohort? | Concurrent observed requests, observed compatible capacity, sustained request pressure, trends, coverage | Investigate a model-specific capacity gap; produce an assumption-based sizing scenario |
| Could existing capacity satisfy more work? | Persistent low measured activity in requested cohorts, GPU-time attribution, memory/CPU context, measurement coverage | Prioritize workload/right-sizing reviews; verify compatibility and service requirements before reclaiming |
| Is placement or fleet mix the issue? | Server-level request headroom distributions, demand concentration, model/cluster imbalance | Review placement/rebalancing candidates and hardware mix; distinguish proxies from actual device-level fragmentation |

The trace alone cannot produce a defensible purchase order or guaranteed savings. It lacks complete unmet/rejected demand, future growth forecasts, per-workload service objectives, per-device placement, hardware interchangeability benchmarks, and procurement/operating prices. Observed placed workload requests are not automatically total demand. A low SM measurement does not prove that requested GPUs can safely be removed.

### Primary dashboard views

1. **Capacity outlook** — GPU-model/cluster capacity versus concurrent observed request demand; sustained pressure and headroom trends; selectable historical window; clear record/measurement coverage. Compute peak/p95 from the time series of summed concurrent demand, not by adding each workload's independent peak or percentile.
2. **Efficiency opportunities** — ranked cohorts requiring review, showing persistent request/activity mismatch, memory/CPU context, duration of the pattern, affected observed GPU-time, and missing-data rates. Output is an evidence-backed investigation queue, not automatic reclaim instructions or guaranteed available GPUs.
3. **Placement and mix** — inventory-preserving server-hour joins, headroom/concentration distributions, model/cluster imbalance, and unmatched records. Server-level headroom is a proxy; exact allocatable device capacity and fragmentation are not established by these tables alone.
4. **Planning scenarios** — explicit user assumptions for demand growth, reserve fraction, and compatible hardware; comparison with a selected observed capacity baseline. No built-in assertion that a new GPU model is interchangeable with a masked source bucket.

Workload type, priority, and model category are cohort filters/drill-downs within these decisions. They are not separate generic explorer pages. Data quality is visible alongside each decision rather than hidden in a technical-only appendix.

### Time scope and denominators

- Main capacity/efficiency planning uses workload and server inventory on the same selected days/hours within 0–184. Validate model and server-hour matching; retain unobserved/missing demand as unknown where appropriate.
- Network is supporting context only for the aligned 109–115 window. It must not share an implied six-month denominator or be used to infer network saturation without link capacities. Keep it as a clearly labeled diagnostic drill-down, not a fleet-wide procurement KPI.
- Execution summary is separate global context for workload lifetime/priority patterns. It has no daily time mapping, is not affected by the daily date filter, and must not enter daily capacity calculations. It is not required on the main planning overview.
- Unknown state/classification and missing SM are retained and quantified. No claim of complete running demand or reclaimable idle GPUs is made when those categories dominate.

### Focused Gold products

| Gold product | Grain | Required measures / purpose |
|---|---|---|
| capacity_demand_hourly | Day/hour/model/cluster | Unique inventory capacity, observed requests split by known/unknown lifecycle state and priority, attributed GPU-time, matched/unmatched coverage |
| activity_evidence_hourly | Hour/cohort | SM sum and valid count, observed GPU-requesting pod-hours, activity distributions, memory/CPU context and coverage; supports persistent-pattern reviews |
| placement_headroom_hourly | Hour/model/cluster with bounded server drill-down | Server-level request headroom/concentration distributions; preserves inventory exactly once |
| opportunity_evidence | Cohort/window | Pattern persistence, affected observations/GPU-time, supporting measures, caveats and validation needed; no guaranteed reclaim quantity |
| planning_scenario_inputs | Model/window | Historical concurrent demand distribution, chosen observed capacity reference, coverage, supported assumption dimensions |
| planning_quality | Dataset/day/cohort | Nulls, unknown labels, key conflicts, invalid/sentinel values and join coverage; gates decision readiness |

Keep numerator/denominator components and mergeable distributions or precomputed supported-window summaries. Do not average means/percentiles indiscriminately or sum daily distinct counts across a window. Avoid an unrestricted dimensional cube that makes saved Gold unnecessarily large.

An illustrative sizing scenario may use `required_slots = ceil(concurrent_demand_anchor * (1 + growth) / (1 - reserve_fraction))`, within a compatible GPU cohort, where 0 <= reserve_fraction < 1. Additional slots are the positive difference from a selected observed capacity baseline. Growth, reserve, service-demand interpretation, compatibility, and the demand anchor are assumptions, not measured recommendations. Unknown/unobserved demand must remain visible; request-based scenarios do not establish performance sufficiency. Optional cost estimates require explicit external/user-supplied prices and a declared cost scope.

### Silver implications before further processing

Retain model, cluster, workload, priority, state, requests, GPU-time, SM/memory/CPU measurements, readiness context, and quality/lineage fields. Preserve a clean base population with a GPU-requesting view, subject to measured storage cost. Aggregate workloads to server/day/hour before joining inventory; start capacity totals from inventory to retain servers with no matched workload records. Quarantine conflicting keys and report unmatched workloads; never multiply capacity by the number of pods. Validate whether apparent spare capacity is observed request headroom, missing workload observations, or a compatible placement opportunity before assigning a planner interpretation.

### Validation and rollout

First define and review the planner metrics, assumptions, and coverage gates. Then implement the proposed Silver/Gold changes and rerun day109 (including the canceled rerun check). Validate the aligned 109–115 window for combined evidence, then expand workload/inventory planning across all days. Resume processing only after strategy approval; EC2 remains stopped.

## Bronze-to-Silver execution plan — implemented and benchmarked

The full-width, Silver-only implementation and its per-table quality rules are documented in [SILVER_RUNBOOK.md](../runbooks/SILVER_RUNBOOK.md). Real results, source/output reconciliation, four final join audits, runtime/cost assumptions and machine alternatives are in [AWS_BENCHMARK_RESULTS.md](../evidence/AWS_BENCHMARK_RESULTS.md). Current entry points are `silver.py` and `gpu_silver_backfill`; the older `pipeline.py` automatically generates pilot Gold and must not be used for this phase.

The tested host is m6a.2xlarge (8 vCPU / 32 GiB), six Spark threads, 12-GiB heap and a 60-GiB gp3 root (3,000 IOPS / 125 MiB/s). The original 30-GiB root hit the 10-GiB free-space guard on the largest day; expansion was explicitly approved. No 500-GiB volume or second worker was created. All 9 benchmark cases and 4 final audits succeeded, and EC2 is verified stopped. Eight unique Silver partitions are committed; 370 remain. The same-source three-thread trial took 363.3 seconds versus 277.4 at six threads; this does not certify smaller-memory or multi-machine behavior.

Full Silver scenario: **16–25 hours**, including measured final audits and a 25% retry allowance at the upper end. Compute is ~$5.61–$8.61 at the refreshed $0.3456/hour rate; a **$7–$11 before-credit allowance** includes public IPv4 and short S3/root retention. New Silver output is projected at ~283–350 GiB from measured ratios. Gold, taxes, prolonged retention and production Airflow/PostgreSQL setup are excluded. The forecast is not a statistical confidence interval or cap. The prior unmeasured 8–24-hour allowance is superseded.

Source semantics remain binding: retain raw fields, nullable/unknown categories and CPU-only rows; do not clip legitimate CPU/request ratios or invent delay/memory/SM units. Footer-proven null-only physical columns are restored to their declared nullable logical types; populated incompatible drift still fails. Quarantine all duplicate/missing server-hour keys for inventory/network; retain/report unproven workload/execution candidate-key repetition rather than selecting arbitrary winners. Every partition reconciles input = accepted + quarantined before committing. Readers select one current manifest, never wildcard all attempts. Old/failed attempts are retained until cleanup review.

Final audits found all sampled workload observations matched inventory; network109 retains 190 unmatched observations. Missing inventory/workload observations remain unknown coverage, not proof of idle capacity. Gold must expose coverage and preserve inventory denominators without fanout. Execution remains global; network remains limited to days109–115.

Before a full run: finalize topology with the user, configure/validate Airflow 3.0.6 LocalExecutor and separate local PostgreSQL metadata, preserve the SQLite pilot history, explicitly apply the tested Spark configuration, prevent legacy Gold-producing DAGs from starting, and replace the two-hour boot/100-minute benchmark guards with an agreed full-run limit. Keep the per-job free-space guard. Reconcile live CPU/root changes with CloudFormation desired state before any stack update. No full backfill or new Gold is authorized yet.

<details>
<summary>Superseded pre-benchmark execution draft (historical assumptions, not current instructions)</summary>

## Bronze-to-Silver execution plan — proposed, not started

This section plans ONLY Bronze-to-Silver. It does not approve an EC2 restart/resize, resumed processing, or Gold generation. The current prototype must be refactored because its entry point automatically generates narrow Silver AND pilot Gold.

### Deliverables and transformation contract

Create four detailed, versioned Silver facts: workload_hourly, server_hourly, network_hourly, execution_summary. Preserve original grains and all useful source fields; retain CPU-only workload records in the clean base and expose GPU-requesting records as a logical view. No cross-table capacity aggregation, procurement scenario, or dashboard metric belongs in this phase.

| Transformation | Required behavior |
|---|---|
| Source validation | Require verified Bronze inventory; check expected schema, types, coverage and source fingerprint; fail on corrupt input or incompatible drift |
| Explicit time/identity types | Materialize day/hour from paths; IDs remain strings; GPU requests remain fractional; execution is explicitly global without invented day/hour |
| Missing/unknown semantics | Preserve unknown categories and nullable workload IDs; distinguish missing measurements from measured zero; retain lifecycle flags independently |
| Numeric quality | Flag non-finite and suspicious values; preserve legitimate CPU/request ratios above1; avoid arbitrary clipping or undocumented conversions |
| Delay handling | Preserve raw delays; add a validity/sentinel flag; normalize a sentinel into a separate analysis field only after verifying its semantics |
| Analysis flags | is_gpu_requesting, has_sm_measurement, missing-classification flags and documented quality flags; lineage/version details in columns or partition manifests |
| Keys and duplicates | Validate inventory/network candidate keys; investigate workload key conflicts; no blind pod_id deduplication across hours or servers |
| Row accounting | Reconcile input = accepted + quarantined rows; ordinary nulls/unknown labels are not automatically rejected; any deduplication needs an approved, separately reconciled rule |
| Cross-table audits | Check workload/inventory and network/inventory server-hour coverage without duplicating capacity; keep base tables separate |
| Output layout | Parquet, partitioned by trace day; retain hour as a column; execution uses a global partition; benchmark ZSTD compression and sensible file sizing |
| Safe publication | Write an immutable attempt, validate it, then publish the per-partition current-version manifest last; readers resolve one committed version rather than wildcarding all attempts |
| Restartability | Ledger of source fingerprint + transformation version + dataset/day status; resume missing/failed work, skip matching validated partitions; clean failed/obsolete attempts after verification |

The initial quality contract must define which issues fail a partition, which records are quarantined, and which are retained with warnings. Every removal needs a reason/count; preserve Bronze unchanged. A failed attempt must not make a partial Silver partition appear ready.

### Tools and roles

- Python3.11 / Java17 / PySpark3.5.5: distributed-within-one-worker DataFrame/SQL transformations, quality aggregates, duplicate/key audits, Parquet writing. Avoid collecting raw data into Python or using Pandas for the full trace.
- Hadoop S3A3.3.4: Spark reads the existing S3 Bronze partitions using the worker's IAM role. No ZIP downloads or 500GiB data disk are required.
- Airflow3.0.6: persistent orchestration, explicit concurrency limits, per-dataset/day retries and task state. Use LocalExecutor with PostgreSQL metadata on the same worker for the longer backfill; preserve the current SQLite pilot database as history rather than treating dags test as the production scheduler.
- PostgreSQL: local orchestration metadata only, not the trace data; no managed RDS instance is proposed.
- Boto3/AWS CLI: source/commit manifests, run ledger and S3 publication/cleanup of named attempts.
- PyArrow: independent output footer/row-count checks and selected value checks. Spark computes full quality summaries; do not download the whole trace to the laptop for validation.
- S3: unchanged Bronze, committed Silver, quarantined records and quality/lineage manifests. Review file sizes after the pilot; an initial 128–256MiB target is tuning guidance, not a guaranteed writer result.

One mapped Airflow task per dataset/day should perform the guarded sequence: check source -> run Spark Silver transform -> verify output/quality -> commit manifest. This avoids needlessly starting Spark for every small validation step. Full coverage has 378 units:185 workload days +185 server days +7 network days +1 global execution partition. The work can be grouped operationally by trace day while retaining dataset-level retry granularity. Gold is a subsequent, separately reviewed phase.

### Recommended EC2 strategy

Use one m6i.2xlarge in Ohio (8vCPU/32GiB) for the measured pilot and, if successful, the Silver backfill. This removes T3 CPU-credit uncertainty and provides heap/cache headroom for keeping all fields and CPU-only rows. Reuse the existing bucket, role and networking; do not provision extra workers, GPU instances, managed Airflow, EMR, NAT gateways or RDS for this phase.

Start with ONE Spark application at a time. A conservative initial configuration is six Spark worker threads and a 12GiB driver heap, leaving CPU/memory headroom for Airflow/PostgreSQL/OS. Tune to eight threads only if pilot measurements show a benefit without control-plane/resource pressure. Do not run eight independent Spark jobs just because the instance has eight CPUs.

Keep the existing 30GiB system disk initially, processing one partition at a time and monitoring spills/free space. Largest source workload day is about2.34GB of Parquet, but compressed input size does not bound memory or spill requirements. Pause new work if free space falls below10GiB. If measurements require more scratch, propose a modest50–80GiB system-volume expansion rather than recreating the500GiB disk. No expansion is authorized now.

A resize requires stop/start. Before any approved restart, disable/reconfigure the existing two-hour boot stop timer so it cannot interrupt a long job. Use a deliberate runtime/budget review threshold and stop compute after the approved Silver job/verification completes. Preserve manifests and Postgres metadata for resume. Parallel day jobs or additional EC2 workers are a later optimization only if the single-worker pilot fails the completion target.

### Pilot, runtime estimate and cost gate

The earlier workload pilot took99.65seconds, but filtered to positive GPU requests and selected only six columns before producing pilot Gold. The new Silver keeps the wider source population and adds quality/key checks. That result is NOT a valid throughput benchmark for the proposed transformation.

Benchmark workload days3 (smallest,1.385GB),109 (reference,2.013GB), and173 (largest,2.337GB), plus representative server/network partitions and the global execution partition. These sizes are from actual ZIP inventory, not guesses. Include all proposed fields/checks/compression in the benchmark. Measure input/accepted/quarantine counts, elapsed time, CPU, peak heap/RSS, S3 throughput, spill and disk usage, output bytes, object counts and correctness. Record startup versus transformation time where possible.

Working scheduling allowance before this benchmark:
- Pilot and resource checks:30–90minutes on the proposed worker; not a measured forecast.
- Full Silver backfill:8–24running hours as a provisional budget envelope, NOT a confidence interval or promise. Actual time may fall outside it; the pilot must replace this estimate before the long run.
- Estimate remaining time from observed throughput/row counts/source bytes across the three workload sizes, add measured overhead for other datasets and validation, and allow for retries. Do not assume linear CPU speedup.

Compute rates were verified through AWS Pricing API earlier November19,2026. The latest attempted refresh failed because the gpu-fleet CLI login expired. Reauthenticate and recheck before execution; no authentication or AWS change is needed merely to review this plan.

| Worker option | Compute/hour | Planning implication |
|---|---:|---|
| Existing t3.large (2vCPU/8GiB) | $0.0832 | Lower hourly rate; full-wide Silver runtime and sustained credit behavior unbenchmarked |
| m6i.xlarge (4vCPU/16GiB) | $0.192 | Alternative if memory/throughput pilot supports it; no measured full-wide runtime yet |
| Proposed m6i.2xlarge (8vCPU/32GiB) | $0.384 | 8–24h envelope implies $3.07–$9.22 compute;30–90min pilot adds about$0.19–$0.58 |

Other charges/assumptions before credits/taxes:
- Public IPv4: $0.005/h, approximately$0.04–$0.12 for8–24h.
- Existing30GiB gp3 root disk: $2.40/month, about$0.08/day while retained; a larger root disk changes this amount.
- Existing Bronze/project S3 storage remains approximately$7.69/month based on the last measured359.1GB bucket size; requests are additional.
- New Silver size is not known. For planning ONLY,330–660GiB would add about$7.59–$15.18/month at$0.023/GiB-month (roughly$0.25–$0.51/day). This is a size scenario, not a measured output or upper bound; ZSTD effectiveness, retained fields and quarantine/version retention must be measured.
- S3 request charges and retry/orphan-attempt storage are additional. Keep compute/S3 in the same region. Do not copy the full trace out of AWS for this run.

Use $15 as an initial operating-budget REVIEW threshold for pilot plus first backfill and short retention, not an enforced cap or guaranteed total. If pilot projection exceeds it or the runtime envelope, pause for review. Keeping all Bronze/Silver for a month costs more: at the illustrative Silver sizes, total storage including the current root disk would be roughly$18–$25/month. Credit applicability and remaining balance are still unverified. Gold processing, later dashboard work, taxes and prolonged retention are excluded.

### Implementation order and approval boundary

1. Review the per-table Silver contracts and quarantine/sentinel rules.
2. Implement a Silver-only entry point, explicit schemas, quality reports, immutable attempts/current manifests, and resumable ledger; test locally before cloud execution.
3. Obtain approval to resize/start the chosen worker; refresh CLI login; validate pricing, boot timer, root-space and actual executor/database setup.
4. Run the representative benchmark; publish a revised runtime/storage/total-cost estimate.
5. Approve the measured backfill scope, execute missing Silver partitions, validate all378 partition commits, preserve logs/manifests, and stop compute.
6. Review capacity-planner Gold definitions using the verified Silver data. Do not run Gold automatically in this phase.

This proposal leaves EC2 stopped and all processing paused. It does not approve new resource charges or execute the old prototype against the full trace.


</details>

## Verified starting point

All four archives have been downloaded and their Parquet members preserved in S3 Bronze. All source CRC and destination key/size inventories passed. Source archive hashes and verification evidence are saved locally and in S3. The temporary 500 GiB EBS volume `vol-028dceeb16c897cfc` was unmounted and deleted at explicit user request; stack `gpu-fleet-data` is deleted. The same worker is retained, now m6a.2xlarge with the approved 60-GiB root after benchmarking. It is confirmed stopped. The full backfill awaits the user's topology decision.

S3-only pilots passed for network day109 (766207 observations), server day109 (775104 server-hours), workload day109 (2816548 GPU-requesting pod-hours), and the complete execution summary (40522321 records). Independent PyArrow checks compared Bronze aggregates and actual S3 Silver row counts with Gold. All four saved reports rendered in the offline dashboard.

A subsequent workload rerun check was canceled when the user requested pause (SSM command `b7a969f7-5d90-4b26-b059-5563524624fe`, Cancelled/137). Treat day109 workload outputs as needing a fresh successful rerun before consuming new results. Its test probes may remain in S3; do not treat them as source data. The canceled Airflow run may still require state cleanup when a persistent scheduler is configured.

## Understand Bronze: real sample rows and insight opportunities

This section was added from **read-only S3 inspection on November 19, 2026**. EC2 remained stopped. No Spark/Airflow processing or layer rewrite was performed for this review.

“Top 10” here means **the first ten physical rows in one selected Parquet file**, not the ten largest values. There is no inherent global ordering in a Parquet dataset. Daily tables use the aligned **day 109, hour 00** partition; execution uses its global file. The rows are examples, not random samples or whole-trace statistics. Extra GPU-requesting workload examples are included because the first ten workload rows happen to be CPU-only.

For readability, hashed IDs are shown as stable aliases (P=pod, S=server, W=workload, C=cluster, A=access-switch group). Aliases only change display; full original IDs and unrounded numbers are retained in the linked CSV/JSON files. See [alias mapping](../bronze_samples/id_aliases.csv). `NULL` is missing data, not zero. CSV blanks represent nulls; JSON is authoritative for original value types and nulls. Independently sampled tables need not share any of these ten IDs: join on the complete keys, never row position.

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

[All ten rows and all columns: CSV](../bronze_samples/pod_10rows.csv) · [JSON, schema, and source provenance](../bronze_samples/pod_10rows.json)

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

[Full GPU example rows: CSV](../bronze_samples/pod_gpu_10rows.csv) · [JSON with provenance](../bronze_samples/pod_gpu_10rows.json)

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

[All ten rows and all columns: CSV](../bronze_samples/server_10rows.csv) · [JSON, schema, and source provenance](../bronze_samples/server_10rows.json)

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

[All ten rows and all columns: CSV](../bronze_samples/network_10rows.csv) · [JSON, schema, and source provenance](../bronze_samples/network_10rows.json)

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

[All ten rows and all columns: CSV](../bronze_samples/execution_10rows.csv) · [JSON, schema, and source provenance](../bronze_samples/execution_10rows.json)

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

The capacity-planner contract above supersedes the earlier broad dashboard wish list. Prioritize the focused Gold products, coverage checks, and planning decisions; use the source examples to preserve useful dimensions rather than exposing every field as a dashboard feature. Network and global execution remain supporting context with separate time scopes.

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

Add public IPv4 ($0.005/h), system disk, and S3 usage. Credit coverage evidence is now supplied and sharing confirmed; see the current benchmark report. Any older amounts in this subsection are historical.

Measured current-worker pilots: workload day109 took 99.65 seconds including Airflow orchestration; server day109 took about 85 seconds; the global execution report took about ten minutes. Extrapolation suggested about ten hours for the older narrow prototype before credit throttling, day variation, retries, or repeated startup costs. That is not a forecast for the proposed full-width Silver contract; use the Bronze-to-Silver benchmark plan above. The earlier 20–50h estimate was conservative and preceded workload measurements. Do not promise a proportional speedup from extra CPUs.

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

1. Agree on the capacity-planner metrics and assumptions; implement the reviewed Silver/Gold design. Inspect canceled workload rerun state and rerun day109 from S3, verifying counts/means, stale-file removal, and neighboring-day preservation.
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
- Which demand anchor, reserve/growth assumptions, opportunity-evidence thresholds, and coverage gates should capacity planners review before the complete trace is processed?

The user's latest instruction is to agree on strategy before starting processing. Preserve that pause even if an earlier resource-choice prompt is answered later; a resource preference alone is not permission to start the backfill.
