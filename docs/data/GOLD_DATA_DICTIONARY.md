# Gold data dictionary

The bundle contains **seven tables and86 physical column occurrences**, including repeated shared keys. Definitions below describe the implemented gold-v1 code and the verified local files, not unimplemented design proposals.

## Read this first

- A pod observation is an accepted source row for a pod observed on a server during an hour. It is not a distinct job or proof of a full hour of running. Repeated candidate keys remain retained.
- Requests observed within the same hour are summed without a Running-state filter. Sequential jobs within that hour can both contribute. These sums are not verified instantaneous concurrency, unmet demand or compute utilization.
- A valid numeric input is non-null, finite and nonnegative. Zero is valid. Negative, NaN and infinite inputs do not enter Gold sums/counts. Headroom itself is allowed to be negative.
- The sum helpers contribute0 for invalid inputs. A sum of0 without a positive matching valid count can mean no eligible readings. Do not interpret it as measured zero activity.
- Stock and time differ: hourly inventory/request fields are GPU slots/equivalents. Summing inventory across hours gives observed GPU-slot-hours. used_gpu_hours is separately attributed GPU-time.
- A count is usually observations, not unique pods, jobs, servers or devices. Never average pre-aggregated means when their denominators differ.
- SM denotes streaming multiprocessor activity. Its released normalization is not established as a0–100percentage here; memory utilization normalization is also unspecified. CPU-request utilization is normalized by requested CPU.

The upstream meanings of raw fields and labels are documented in [Alibaba’s schema](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-gpu-v2026/docs/schema.md). Gold grouping, filtering and calculation rules are taken from `gold.py`; storage/export rules come from `ops/export_gold.py`. The implementation is authoritative for these derived columns.

## Release metadata

| Item | Value |
|---|---|
| Release / version | gold_20261120 / gold-v1 |
| Observation coverage | Relative days0–184; network109–115; execution global |
| Plan created | 2026-11-21T00:05:06.753237+00:00 |
| Release published | 2026-11-21T08:55:03.564061+00:00 |
| Processing dates | These UTC timestamps describe our pipeline, not trace calendar dates |
| Sources pinned |378 Silver manifests:185 inventory,185 pod,7 network,1 execution |
| Gold units |185 daily units plus1 global unit; exported into7 Parquet files |
| Storage | Local ZSTD Parquet, consolidated with Arrow26.0.1; retained Spark3.5.5 schema metadata; actual Arrow types below |
| Bundle size |47,379,950 bytes for9 manifest-listed files; bundle.json itself is additional |
| Rows |3,444,237 aggregate rows across all tables; not distinct source observations |
| GPU / cluster labels |13 released GPU cohorts;17 stable hashed clusters |
| Ordering |Daily outputs concatenated; within-day Spark group order is unspecified. File order is not chronological |
| Grain verification |All7 exported tables have unique rows at their declared grouping keys in this release |
| Nullability |Parquet “nullable” means permitted; actual null counts are measured separately |

Physical type legend: int32/int64 =32/64-bit integer; double =64-bit floating point; bool =true/false; string =UTF-8 text. Counting server-hours in a single-hour row gives servers for that hour, but summing across hours repeats those servers.

## Table overview

| Table | Rows | Columns | Parquet bytes |
|---|---:|---:|---:|
| capacity_hourly | 240,846 | 23 | 6,890,171 |
| activity_evidence_hourly | 186,884 | 17 | 10,465,007 |
| workload_mix_hourly | 2,789,881 | 11 | 23,257,294 |
| planning_quality | 191,951 | 8 | 1,503,586 |
| placement_daily | 26,918 | 5 | 101,598 |
| network_context_hourly | 7,595 | 9 | 184,298 |
| execution_context_global | 162 | 13 | 14,779 |

## 1. capacity_hourly

Inventory and observed request evidence for a GPU/cluster hour. Inventory is the left side of the join, preserving servers without pod observations.

**Grain / unique key:** `day, hour, gpu_spec_public, cluster_id`.
**Coverage:** Days0–184, grouped by released hour.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `hour` | int32 | Released hour within a trace day, 0–23. The original trace time zone is not supplied here. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `inventory_gpu_slots` | int64 | Sum of finite, nonnegative inventory gpu_count, counting each accepted server-hour once. Zero is a valid recorded count. Null if the group has no valid capacity values. Missing/invalid servers are not estimated. |
| `valid_capacity_server_hours` | int64 | Number of inventory rows with a valid nonnegative gpu_count, including recorded zero. Compare with inventory_server_hours to assess capacity-value coverage. |
| `inventory_server_hours` | int64 | Number of unique accepted inventory server/hour rows in this group. Because this row represents one hour, it is also the observed server count for that hour. |
| `servers_with_pod_observations` | int64 | Inventory server-hours with at least one matched accepted pod row, including CPU-only pods. Presence of one row does not prove complete workload capture. |
| `matched_observed_gpu_requests` | double | Sum of valid nonnegative requested GPU-equivalent counts on matched server-hours, regardless of pod/inventory label conflicts. CPU-only zero requests are included. Not filtered to Running. |
| `compatible_observed_gpu_requests` | double | Same request sum, but excludes an entire server-hour if its pod model/cluster labels conflict, are missing, or contain multiple variants. It is not GPU-time-weighted or a verified instantaneous/running demand measure. |
| `compatible_valid_request_rows` | int64 | Valid request-value row count from those compatible server-hours. Includes zero-GPU requests; not a GPU count or distinct-pod count. |
| `matched_pod_observations` | int64 | All accepted pod rows on matched inventory server-hours, even when requests are invalid or labels conflict. Repeated observations remain repeated. |
| `valid_request_rows` | int64 | Number of those matched pod rows whose request is finite and nonnegative. Includes zero requests, even on label-conflicted server-hours. |
| `mismatched_server_hours` | int64 | Count of matched server-hours with any missing/conflicting pod model/cluster label or multiple variants. Inventory-only servers are not called mismatches. One bad pod can disqualify the whole server-hour for compatible-request sums. |
| `over_requested_server_hours` | int64 | Number of headroom-eligible server-hours where inventory GPU slots minus summed valid requests is negative. This counts server-hours, not missing GPUs. No rounding tolerance is applied around zero. |
| `headroom_evidenced_server_hours` | int64 | Count with computed non-null request headroom. Requires pod observations, every request valid, compatible labels, valid capacity, no known anomaly hour and no duplicate-candidate group anywhere in that pod day. |
| `min_observed_headroom` | double | Smallest inventory-minus-request value among eligible server-hours; can be negative. Null if none qualify. It is not total free capacity. Eligible headroom can come from CPU-only observed pods with zero GPU requests; workload completeness is still unverified. |
| `max_observed_headroom` | double | Largest inventory-minus-request value among eligible server-hours. Null if none qualify. Min/max cannot reconstruct total positive headroom or device placement. |
| `partial_request_to_inventory_ratio` | double | compatible_observed_gpu_requests / inventory_gpu_slots, only when inventory is positive and compatible_valid_request_rows>0; otherwise null. May exceed 1. Coverage, lifecycle and within-hour overlap remain unverified. |
| `source_anomaly` | bool | True for a source-observation-catalog hour, propagated to all inventory cohort groups for that hour. Here: day5/hour06. It flags evidence; it does not erase the raw sums. |
| `duplicate_candidates_present` | bool | True when that entire pod day has any repeated candidate key, even outside this cohort/hour. Data is retained; the flag does not establish that repeated records are erroneous duplicates. |
| `decision_ready` | bool | Always false in gold-v1. It is a fixed policy flag, not an algorithmically calculated procurement score. |
| `eligibility_reason` | string | Policy text explaining that request/lifecycle completeness is unverified and results are observational evidence. |

**Actual nulls:** `min_observed_headroom`: 108,819; `max_observed_headroom`: 108,819; `partial_request_to_inventory_ratio`: 48,895
**Actual coverage detail:** all48,895 null request-ratio groups have no pod observations and do have positive recorded inventory. All6,570,124,684 matched pod observations have valid request values, including zero. Weighted server-hour record presence is98.799%; that is not demand completeness.

**Dashboard use:** Ranks p95 hourly request/inventory ratios; provides inventory/request series, screening flags and exploratory request anchors/capacity references.

**Example:** day0/hour0/A100/cluster2f961135… has3,648 slots and3,832 compatible observed requests:3832/3648=1.05044. All456 inventory servers have some pod records;79 eligible server-hours are over-requested. This does not prove184 extra GPUs are needed. The sum includes unverified lifecycle states and does not prove simultaneous running.

## 2. activity_evidence_hourly

Telemetry components for positive-GPU-request pod observations. Unmatched records are retained in separate match groups; the dashboard selects matched, compatible rows.

**Grain / unique key:** `day, hour, gpu_spec_public, cluster_id, inventory_match, cohort_match`.
**Coverage:** Days0–184, grouped by released hour.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `hour` | int32 | Released hour within a trace day, 0–23. The original trace time zone is not supplied here. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `inventory_match` | bool | True when the pod server/day/hour key exists in accepted inventory. False means no matched inventory key. This is independent of whether the GPU model is known. |
| `cohort_match` | bool | True only when inventory matched, its GPU/cluster labels are non-null, and the pod labels agree. It requires inventory_match=true. This is per pod row before aggregation. |
| `gpu_requesting_observations` | int64 | Count of accepted pod-hour rows with finite gpu_request>0 in this group. Fractional requests qualify; CPU-only zero requests do not. Not distinct pods/jobs. |
| `observed_gpu_requests` | double | Sum of valid requested GPU-equivalent counts across those positive-request observations. Not restricted to Running and not time-weighted. |
| `sm_source_unit_sum` | double | Sum of finite, nonnegative avg_gpu_sm_util readings. It adds pod-hour averages in released source units; it is neither a percentage nor a GPU-weighted fleet statistic. |
| `sm_valid_observations` | int64 | Number of eligible SM readings, including measured zero. Mean SM = sm_source_unit_sum / this count; if zero, the mean is unknown. |
| `gpu_memory_gib_sum` | double | Sum of valid avg_gpu_mem_gib readings. These are pod-hour average GiB values added across observations, not a simultaneous fleet-memory total or GiB-hours. |
| `gpu_memory_valid_observations` | int64 | Count of valid nonnegative GPU-memory readings. Divide the corresponding sum by this count for an observation-weighted mean GiB. |
| `cpu_request_util_sum` | double | Sum of valid avg_cpu_request_util readings. The upstream field is CPU utilization normalized by requested CPU; it is a ratio, not cores or a fleet CPU-utilization percentage. |
| `cpu_util_valid_observations` | int64 | Count of eligible CPU-request-utilization readings. Their sum/count gives a mean ratio; readings above1 are retained. |
| `memory_util_sum` | double | Sum of finite nonnegative avg_memory_util readings in source units. The released schema does not specify the normalization sufficiently to call this a percentage. |
| `memory_util_valid_observations` | int64 | Count of eligible memory-utilization readings. Their sum/count gives an observation mean, not a fleet memory-utilization ratio. |
| `reported_gpu_hours` | double | Sum of valid used_gpu_hours attribution for this positive-request population. GPU-hours are a separate time-based quantity; they need not equal GPU requests. There is no corresponding valid-GPU-hours count in this table. |

**Actual nulls:** None in this exported table. Zero valid counts still represent missing/ineligible measurements.
**Dashboard use:** Computes source-unit means and measurement coverage, then screens persistent low activity in the selected cohort.

**Example:** the first XPU-A row has SM sum0 and valid count0. Its mean is unknown. The next XPU-E row has675.5404/79≈8.5511 source units. Neither value is a fleet GPU-utilization percentage.

**Population / coverage:**524,624,662 positive-GPU-request observations;390,319,456 have valid SM.25,604 aggregate rows have zero valid SM readings. Requests can be fractional, so request totals and row counts differ. GPU-hours have no valid-reading counter here.

## 3. workload_mix_hourly

Five independent category breakdowns of accepted pod observations. All accepted rows are included, including CPU-only records.

**Grain / unique key:** `day, hour, gpu_spec_public, cluster_id, inventory_match, cohort_match, dimension, value`.
**Coverage:** Days0–184, grouped by released hour.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `hour` | int32 | Released hour within a trace day, 0–23. The original trace time zone is not supplied here. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `inventory_match` | bool | True when the pod server/day/hour key exists in accepted inventory. False means no matched inventory key. This is independent of whether the GPU model is known. |
| `cohort_match` | bool | True only when inventory matched, its GPU/cluster labels are non-null, and the pod labels agree. It requires inventory_match=true. This is per pod row before aggregation. |
| `dimension` | string | Which single breakdown this row belongs to: priority_class, job_type_public, model_type_public, state_public or is_genai_request. These are five separate marginal views of the same population. |
| `value` | string | Released label for that dimension, cast to string. GenAI booleans become "true"/"false"; null would stay null. Literal "Unknown" and "unknown" remain source labels. |
| `observations` | int64 | Count of accepted pod-hour rows in this category, including CPU-only rows and retained duplicate candidates. Not distinct workloads. |
| `observed_gpu_requests` | double | Sum of finite nonnegative GPU-equivalent requests for the category, including zero. Filter to one dimension before summing categories. |
| `valid_request_observations` | int64 | Count of category rows with valid requests, including zero. observations minus this count gives ineligible request-value rows. |

**Actual nulls:** None in this exported table. Zero valid counts still represent missing/ineligible measurements.
**Dashboard use:** Optional Workload mix detail, one selected dimension at a time.

**Do not add dimensions:** each accepted pod contributes to all five dimension families. Each family sums to6,570,124,684 observations. Summing all families counts that population five times. Gold retains marginal distributions, not joint priority×job×model combinations.

## 4. planning_quality

Accepted pod populations split by inventory/cohort matching and request validity. This table is not the full Silver quality report.

**Grain / unique key:** `day, hour, gpu_spec_public, cluster_id, inventory_match, cohort_match`.
**Coverage:** Days0–184, grouped by released hour.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `hour` | int32 | Released hour within a trace day, 0–23. The original trace time zone is not supplied here. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `inventory_match` | bool | True when the pod server/day/hour key exists in accepted inventory. False means no matched inventory key. This is independent of whether the GPU model is known. |
| `cohort_match` | bool | True only when inventory matched, its GPU/cluster labels are non-null, and the pod labels agree. It requires inventory_match=true. This is per pod row before aggregation. |
| `observations` | int64 | Count of all accepted pod-hour rows in the matching/cohort category, including CPU-only rows. Its population is broader than activity_evidence_hourly. |
| `valid_request_observations` | int64 | Count with finite nonnegative request values, including zero. This measures request-value validity, not complete workload capture, readiness or correctness of every telemetry field. |

**Actual nulls:** None in this exported table. Zero valid counts still represent missing/ineligible measurements.
**Dashboard use:** Quality detail: matching populations and valid request-value counts.

**Actual release:** all6,570,124,684 pod observations are in inventory_match=true/cohort_match=true groups. This says keys/labels matched; it does not prove complete demand, known state or valid SM. Source anomalies and candidate-duplicate flags are in capacity_hourly and lineage, not columns in this table.

## 5. placement_daily

Daily counts of server-hour headroom classes. Headroom is computed at server/day/hour before binning; this is not a server-level drill-down.

**Grain / unique key:** `day, gpu_spec_public, cluster_id, headroom_bin`.
**Coverage:** Days0–184, hourly server evidence binned and rolled up to day.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `headroom_bin` | string | Request-headroom class for each inventory server-hour before daily grouping: negative (<0), zero (=0), (0,1], (1,4], >4, or unknown. Unknown includes missing/incompatible/invalid/anomalous/duplicate-candidate evidence. Floating-point arithmetic has no zero tolerance. |
| `count` | int64 | Number of server-hour observations in this day/cohort/cluster/bin. A server can contribute once each hour and appear in different bins during a day. This is not distinct servers, pods or GPU slots. |

**Actual nulls:** None in this exported table. Zero valid counts still represent missing/ineligible measurements.
**Dashboard use:** Labeled headroom bars for the selected day range/cohort/cluster. Gold has already put ineligible evidence into unknown.

**Example:** count5,701 in the zero bin means5,701 server-hour records with exactly zero request headroom. It is not5,701 unique servers or GPUs. Sum bin counts for a day/cohort/cluster to obtain inventory server-hours.

## 6. network_context_hourly

Hourly network averages summarized by the matched inventory cohort. The short network window is independent of the185-day workload window. Group keys come from inventory; unmatched records lose individual server identity in this aggregate.

**Grain / unique key:** `day, hour, gpu_spec_public, cluster_id`.
**Coverage:** Days109–115, all24 hourly buckets.

| Column | Type | Meaning |
|---|---|---|
| `day` | int32 | Relative trace day, stored as an integer. 0 is the first released day; this is not a calendar date. |
| `hour` | int32 | Released hour within a trace day, 0–23. The original trace time zone is not supplied here. |
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `cluster_id` | string | Stable anonymized cluster ID within this trace release. Not a region, tenant, server count or readable cluster name. Dashboard Cluster 01…17 are display aliases. |
| `observations` | int64 | Number of accepted server-hour network records in this cohort group. Records without inventory assignments remain grouped under null GPU/cluster labels. |
| `rx_gibps_sum` | double | Sum of valid average receive rates reported in GiB/s across those server-hour observations. Not a transfer volume or saturation percentage. |
| `rx_valid_observations` | int64 | Count of eligible receive-rate readings, including zero. Mean receive rate per observed server = rx_gibps_sum / this count. |
| `tx_gibps_sum` | double | Sum of valid average transmit rates in GiB/s. Not bytes transferred; average rates also do not establish within-hour bursts. RX+TX can count both ends of internal traffic and is not an Internet-egress bill. |
| `tx_valid_observations` | int64 | Count of eligible transmit-rate readings. Mean transmit rate per observed server = tx_gibps_sum / this count. |

**Actual nulls:** `gpu_spec_public`: 168; `cluster_id`: 168
**Dashboard use:** Optional Network detail, only the selected overlap with109–115; means use their matching denominators.

**Actual missing assignment:**168 rows have null model and cluster labels—one aggregate per hour in the seven-day window—covering1,940 unmatched network observations. All5,390,218 accepted network records remain represented. Null-label groups cannot be assigned to a known cohort from this Gold output.

**Example:** RX sum411.6586 over641 valid server observations gives≈0.6422 GiB/s per observed server. Summing hourly means is not a mean; bytes over time require an explicit duration conversion and completeness assumptions.

## 7. execution_context_global

Global execution-span and delay components. There is no daily/cluster mapping in this output, so exact alignment to a selected daily window is not established.

**Grain / unique key:** `gpu_spec_public, priority_class, job_type_public`.
**Coverage:** Global, no day/hour/cluster columns.

| Column | Type | Meaning |
|---|---|---|
| `gpu_spec_public` | string | Released GPU-model/cohort label. Capacity, placement and network use inventory labels; activity, mix and quality use pod labels; execution uses its own source labels. XPU labels are anonymized; do not assume cross-model performance equivalence. |
| `priority_class` | string | Released priority bucket: HP, LP or Other. This is a category, not a numeric priority or a capacity allocation denominator. |
| `job_type_public` | string | Released job-type label for the observed execution record. Unknown stays explicit; it is not inferred from runtime or priority. |
| `observed_execution_records` | int64 | Count of accepted execution-summary rows in this group. Not distinct workload IDs, verified completed jobs or daily executions. Gold applies no additional GPU-request filter to this Silver input. |
| `observed_span_hours_sum` | double | Sum of finite nonnegative duration_hours values for observed execution spans. Not an end-to-end service latency, GPU-hours or guaranteed completed runtime. |
| `valid_span_records` | int64 | Count of eligible observed-span values, including zero. Mean observed span hours = observed_span_hours_sum / this count. |
| `schedule_delay_sec_sum` | int64 | Sum of valid nonnegative scheduling delays in seconds. Negative/sentinel candidates and missing values are excluded, not changed to zero. |
| `valid_schedule_delay_records` | int64 | Number of eligible scheduling-delay readings. Use this, not all execution records, to compute the mean delay. |
| `ready_delay_sec_sum` | int64 | Sum of valid nonnegative delays from scheduling to readiness, in seconds. It is not submit-to-completion latency. |
| `valid_ready_delay_records` | int64 | Number of eligible readiness-delay values. It is independent of the ready_status counts; no equivalence between status and a valid delay is imposed. |
| `ready_true_records` | int64 | Number of source execution rows with ready_status=true. Readiness is not successful completion. |
| `ready_false_records` | int64 | Number with ready_status=false. False alone does not establish failure, reason or delay duration. |
| `ready_unknown_records` | int64 | Number with ready_status=null. These remain unknown rather than being folded into false. |

**Actual nulls:** None in this exported table. Zero valid counts still represent missing/ineligible measurements.
**Dashboard use:** Optional Execution detail. GPU cohort applies; day/cluster filters do not.

**Actual population:**40,522,321 accepted execution-summary records. The three ready-status counts sum to the observed_execution_records for each row. Valid-delay counts need not equal ready_true_records: source status and numeric-delay validity are separate fields. No percentile distribution is retained here; sums/counts alone cannot reconstruct p95 execution time.

## Category labels

| Dimension | Released meaning / observed values |
|---|---|
| priority_class | HP: guaranteed high-priority class; LP: low-priority/spot; Other: outside those bands. Labels are not proof of observed SLA performance |
| job_type_public | training, online_inference, offline_inference, dev, other, unknown |
| model_type_public | genai, rec (recommendation), cv (vision), embedding, dev, unknown |
| state_public | This export contains only Unknown and Standby. The declared source vocabulary is broader; no Running records appear in these Gold marginals |
| is_genai_request | String values true/false in mix output; released detection flag, not an inference made by Gold. False is not proof the workload cannot be GenAI |

**Measured completeness matters:** Unknown state represents99.808% of pod observations and99.001% of observed GPU-request weight. Unknown job type represents92.055% of observations but only0.511% of request weight. CPU-only rows dominate many record-count denominators; do not interpret these as percentages of GPU-hours. These full-release values include flagged observations; the dashboard’s screened window can differ.

**GenAI marginals differ:** model_type_public=genai covers246,344,068 observations, while is_genai_request=true covers221,152,429. Gold preserves both labels independently. Their cross-tabulation cannot be recovered from these separate marginal products.

## Quality flags

-53 relative days contain repeated candidate keys. Gold does not decide they are proven erroneous duplicates or remove a winner. The day-wide flag is conservative: one repeated candidate group flags every cohort/hour of that day.
- Day5/hour06 has zero requests but some positive GPU activity in its source file. source_anomaly flags the hour; raw sums remain. Do not call it idle capacity.
- Request ratios do not automatically disappear on flagged rows. The dashboard screens flags by default. Headroom is stricter: an anomalous hour or any duplicate-candidate group anywhere in the day makes its server headroom unknown.
- Arithmetic tests use exact zero and raw sign, with no rounding tolerance. Very small floating-point residuals around zero can affect a bin; inspect magnitude before treating them as meaningful deficits.
- All decision_ready values are false by design. Schema/row checks establish technical consistency, not procurement readiness.

## Safe joins

Let D=(day,hour,gpu_spec_public,cluster_id).

| Relationship | Safe approach | Risk to avoid |
|---|---|---|
| Capacity ↔ activity/quality | Include match flags, or first filter to inventory_match=true/cohort_match=true so there is at most one row per D | The generic products can hold several match groups per D; unmatched pod labels are not authoritative inventory cohorts |
| Capacity ↔ mix | Choose one dimension and category policy; aggregate requests before joining capacity at D | Raw category rows repeat capacity; joining two dimensions does not reconstruct joint pod classifications |
| Capacity ↔ network | D, only109–115, preserving unmatched/missing observations separately | Joining on model or day alone fans out rows; missing days are not zero traffic |
| Placement ↔ hourly products | Roll up to day/model/cluster first, and keep each bin’s denominator once | Daily bins cannot be joined to each hour or summed as distinct servers |
| Execution ↔ daily products | Keep execution as separate global context | No day/hour/cluster key exists. Joining only by model repeats global totals and invents temporal alignment |

Capacity uses server-hour-level compatibility: one conflicting pod label suppresses the entire server-hour from compatible request sums. Activity/mix/quality use per-pod match flags. They need not reconcile exactly in a release with conflicts; this release’s accepted pod rows all matched.

## Correct calculations

| Measure | Calculation / condition |
|---|---|
| SM observation mean | sum(sm_source_unit_sum) / sum(sm_valid_observations); denominator0 → unknown |
| Mean GPU memory | sum(gpu_memory_gib_sum) / sum(gpu_memory_valid_observations) |
| Mean CPU-request ratio | sum(cpu_request_util_sum) / sum(cpu_util_valid_observations), not average hourly means |
| SM coverage | sum(sm_valid_observations) / sum(gpu_requesting_observations) |
| Inventory record presence | sum(servers_with_pod_observations) / sum(inventory_server_hours); not full-demand completeness |
| Window request pressure | Build each eligible hour’s summed compatible requests and inventory, then compute its ratio. Calculate p95 across that hourly series; do not average group p95 values |
| Mean schedule delay | sum(schedule_delay_sec_sum) / sum(valid_schedule_delay_records) |
| Unknown headroom share | count in unknown bin / sum(count over all bins), at the same selected grain |

GPU-request sums and GPU-hour attribution answer different questions. An illustrative two-GPU request observed for half an hour can carry request2 but attributed GPU-time1. The source does not establish complete demand from either quantity alone.

## Metadata files

| File / field | Meaning |
|---|---|
| bundle.json | Local export catalog: release, relative_days, metric policy, table rows/columns, file bytes/SHA256 and total_bytes. It is not itself included in its own file-hash list |
| plan_identity / identity | SHA256 of the canonical pinned release plan subset: version, release, days, code hash and full source manifests. Binds one output release to one source/code selection |
| code_sha256 | Hash of gold.py used for this release—not every helper, UI file or Git commit |
| silver_code_sha256 | Hash of silver.py for the pinned Silver manifests |
| lineage.json → plan | version, release, days, synthetic flag, code identities,378 source manifests, creation UTC timestamp, metric policy and identity |
| lineage.json → release | Published version/release identity, code hash,186 unit manifests, source days, scope and publish UTC timestamp |
| Unit manifest | unit (day=N or global), plan identity, code hash, created UTC, per-table paths/inventories/row counts/schema and quality report |
| Daily quality | Inventory reconciliation result, observed GPU-slot-hours, duplicate-candidate-group count, source observations/anomaly hours, policy readiness and source accepted/quarantined/input row counts |
| Source manifest | Accepted/quarantine paths, original source fingerprint, Silver code hash, row/column quality diagnostics and output inventories for the selected source attempt |
| Output inventory | Object path, size and identity. S3 identity is an ETag, which is not universally a plain-file MD5; local export SHA256 is a separate byte-integrity check |
| METRICS.md | Short interpretation and aggregation rules shipped with the bundle |
| Parquet metadata | Arrow physical field types, allowed nullability, row groups, compression and retained Spark3.5.5 schema metadata. Not foreign-key constraints or a database-enforced primary key |
| GOLD_METADATA_PROFILE.json | Local measured metadata: every column’s actual null count/type, numeric extrema, categorical values, row-group counts/row counts and verified uniqueness at the stated grain |

Gold file hashes detect changes relative to the exported manifest; they are not independent proof of AWS origin. Saved AWS run/status evidence provides the execution trail. No AWS requests were needed to create this dictionary.

## What Gold cannot show

No pod/server/workload IDs, individual executions, device placement, exact simultaneous demand, unique workload counts across windows, unmaterialized distributions, link capacities, GPU prices or hardware-equivalence mapping are retained in these seven aggregates. More detailed questions require the preserved Silver/source data or new Gold products. Do not infer missing fields from these summaries.
