# Silver column contracts and edge-case coverage

This contract follows the failed day5 run. Representative days did not establish whole-trace physical schemas. The restart gate is a footer audit of every Bronze file plus regression tests; row-level checks still run on each partition. This is not a claim that every possible semantic edge case has been proven.

## Shared rules

- Read each file using its native physical schema. Restore footer-proven null-only fields to typed nulls. Reject missing/extra columns and incompatible populated types.
- Allow signed int32 → int64, float32 → float64, and integer → double only within the exact integer range ±2^53. Never silently narrow floats into integer capacities or coerce strings/booleans into numeric values.
- Preserve every raw field, CPU-only rows, fractional requests and ordinary nulls. Count nulls for every column; count blank/Unknown literals for every string; distinguish NaN/+Infinity/-Infinity for every double and true/false/null for every boolean.
- Negative numeric values are retained with flags. Delay sentinel meanings and uncertain units are not invented. Eligible Gold metrics must apply explicit quality predicates.
- Inventory/network invalid server keys and all duplicate server-hour-key rows are quarantined; unproven workload/execution duplicate candidates are retained/reported. Reconcile input = accepted + quarantined. No arbitrary deduplication winner.
- Fail before commit on unreadable input, unsupported drift, unsafe conversion or failed row accounting. Publish the immutable attempt first and the current pointer last.

## Per-column rules

### pod (23 physical columns)

| Column | Canonical type | Edge-case rule |
|---|---|---|
| `pod_id` | `string` | Opaque string, not a number. Candidate uniqueness is audited at the documented grain; no cross-hour blind deduplication. |
| `workload_id` | `string` | Nullable workload grouping identity. Nulls are retained; not a unique join key and must not become an empty string. |
| `server_id` | `string` | Join identity with day/hour. Inventory/network missing or blank server keys and ALL duplicate server-hour rows are quarantined; workloads/execution missing identities are retained and reported. |
| `cluster_id` | `string` | Opaque cohort identity; missing/blank values remain explicit. Do not invent a cluster or join by row position. |
| `state_public` | `string` | Open categorical vocabulary. Unknown is not failed or inactive. Preserve and report unknown/null/blank observations. |
| `gpu_spec_public` | `string` | Open/masked model categories; no inferred vendor identity or automatic hardware interchangeability. |
| `server_gpu_count` | `bigint` | Capacity hint from workload record; do not multiply into fleet capacity across pods. Zero and null remain distinct. |
| `server_cpu_capacity_cores` | `bigint` | Preserve source capacity hint; do not multiply across workload rows. Negative values flagged. |
| `priority_class` | `string` | Open categorical vocabulary. Preserve labels; count null/blank/Unknown without inventing priority ordering. |
| `job_type_public` | `string` | Open categorical vocabulary. Do not reject a new label or equate workload types for performance analysis. |
| `model_type_public` | `string` | Open/masked categorical vocabulary; null and Unknown remain distinct and visible. |
| `is_genai_request` | `boolean` | Three states: true, false, unknown. Never fill null with false or infer from job/model label. |
| `gpu_request` | `double` | Preserve fractional requests and zero-GPU rows. Missing/non-finite request => unknown GPU-view membership. Negative requests are flagged, not clipped. |
| `gpu_mem_request` | `bigint` | Preserve raw integer and nulls; units remain unconfirmed. No conversion into bytes/GiB or comparison with measured GiB without a verified unit definition. |
| `cpu_request_cores` | `double` | Preserve fractional requests and zeros. Negative/non-finite values flagged; no arbitrary upper cap. |
| `used_gpu_hours` | `double` | Preserve partial-hour/fractional attribution. Do not assume equality to instantaneous request or infer elapsed running time. |
| `avg_gpu_sm_util` | `double` | Preserve raw source measurement; finite presence is separate from non-negative/eligible quality. No unverified range clipping or automatic reclaim interpretation. |
| `avg_gpu_mem_gib` | `double` | Preserve measured GiB and nulls; do not compare to request field until request units are verified. |
| `avg_cpu_request_util` | `double` | Ratios above 1 are allowed; never clamp to 1. Missing, measured zero and non-finite values stay distinct. |
| `avg_memory_util` | `double` | Preserve raw source units; do not assume percent, clamp, or fill missing readings with zero. |
| `ready_status` | `boolean` | Three states: true, false, unknown. Retain independently from delay/sentinel fields; do not impose unverified equivalences. |
| `schedule_delay_sec` | `bigint` | Retain raw negative/sentinel candidates with a negative flag. Do not convert -1 to zero or infer failed status. |
| `ready_delay_sec` | `bigint` | Retain raw negative/sentinel candidates with a negative flag, even when ready_status is true. Semantics require source confirmation. |

### server (6 physical columns)

| Column | Canonical type | Edge-case rule |
|---|---|---|
| `server_id` | `string` | Join identity with day/hour. Inventory/network missing or blank server keys and ALL duplicate server-hour rows are quarantined; workloads/execution missing identities are retained and reported. |
| `cluster_id` | `string` | Opaque cohort identity; missing/blank values remain explicit. Do not invent a cluster or join by row position. |
| `asw_id` | `string` | Opaque switch grouping. Null-only Parquet columns occur in real data; restore typed nulls, do not populate an invented switch. |
| `gpu_spec_public` | `string` | Open/masked model categories; no inferred vendor identity or automatic hardware interchangeability. |
| `gpu_count` | `bigint` | Capacity denominator only after unique server/hour validation. Zero is retained; null is unknown; negatives require quality filtering. |
| `cpu_capacity_cores` | `bigint` | Capacity denominator only after unique server/hour validation. Zero and null remain distinct; negatives flagged. |

### network (3 physical columns)

| Column | Canonical type | Edge-case rule |
|---|---|---|
| `server_id` | `string` | Join identity with day/hour. Inventory/network missing or blank server keys and ALL duplicate server-hour rows are quarantined; workloads/execution missing identities are retained and reported. |
| `rx_gibps_avg` | `double` | Hourly mean receive rate; released unit is GiB/s. No inference of link utilization/saturation without link capacities. |
| `tx_gibps_avg` | `double` | Hourly mean transmit rate; released unit is GiB/s. Missing is not zero and hourly means do not reveal bursts. |

### execution (14 physical columns)

| Column | Canonical type | Edge-case rule |
|---|---|---|
| `pod_id` | `string` | Opaque string, not a number. Candidate uniqueness is audited at the documented grain; no cross-hour blind deduplication. |
| `workload_id` | `string` | Nullable workload grouping identity. Nulls are retained; not a unique join key and must not become an empty string. |
| `server_id` | `string` | Join identity with day/hour. Inventory/network missing or blank server keys and ALL duplicate server-hour rows are quarantined; workloads/execution missing identities are retained and reported. |
| `gpu_spec_public` | `string` | Open/masked model categories; no inferred vendor identity or automatic hardware interchangeability. |
| `priority_class` | `string` | Open categorical vocabulary. Preserve labels; count null/blank/Unknown without inventing priority ordering. |
| `job_type_public` | `string` | Open categorical vocabulary. Do not reject a new label or equate workload types for performance analysis. |
| `model_type_public` | `string` | Open/masked categorical vocabulary; null and Unknown remain distinct and visible. |
| `is_genai_request` | `boolean` | Three states: true, false, unknown. Never fill null with false or infer from job/model label. |
| `gpu_request` | `double` | Preserve fractional requests and zero-GPU rows. Missing/non-finite request => unknown GPU-view membership. Negative requests are flagged, not clipped. |
| `duration_hours` | `double` | Observed execution span, not necessarily completed-job runtime. Zero retained, negative/non-finite flagged; no invented daily mapping. |
| `schedule_delay_sec` | `bigint` | Retain raw negative/sentinel candidates with a negative flag. Do not convert -1 to zero or infer failed status. |
| `ready_delay_sec` | `bigint` | Retain raw negative/sentinel candidates with a negative flag, even when ready_status is true. Semantics require source confirmation. |
| `ready_status` | `boolean` | Three states: true, false, unknown. Retain independently from delay/sentinel fields; do not impose unverified equivalences. |
| `schedule_status` | `boolean` | Three states: true, false, unknown. Retain independently from observed delay/span fields. |

## Derived fields and joins

Daily tables materialize integer day/hour from verified paths, require all24hours and valid dataset coverage, and join on server/day/hour after inventory uniqueness checks. Execution remains global without invented day/hour. Missing matching inventory is recorded as unknown coverage, not silently removed or treated as zero. Capacity denominators must start from unique inventory, avoiding workload fanout.

## What the whole-trace preflight proves—and does not prove

The footer preflight enumerates every file and physical column type, schema variant, metadata null counts, extrema and statistics coverage. Missing statistics remain unknown. Footer statistics do not establish NaN/Infinity row counts, duplicate keys, conditional status/delay meaning, or join coverage. Spark performs those implemented row checks during transformation; semantic assumptions not supported by source documentation remain explicit limitations.

New tests cover mixed integer/double/float32 encodings, rejection outside exact integer-to-double range, null-only/mixed-null files, NaN versus null/zero, boolean counts, unknown strings, duplicate-key quarantine, row reconciliation, unchanged previous commits after failure, and guarded task termination.
