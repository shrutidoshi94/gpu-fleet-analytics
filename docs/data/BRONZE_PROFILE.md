# Whole-trace Bronze metadata profile

All 9,049 original Parquet file footers were inspected before recovery. These are complete file/schema inventories and footer-derived statistics, not proof of every row-level semantic invariant. The profiling scan does not mean the full Bronze-to-Silver transformation has finished.

| Dataset | Files | Rows recorded in footers | Schema variants | Covered relative days |
|---|---:|---:|---:|---:|
| pod | 4,440 | 6,570,124,684 | 2 | 185 |
| server | 4,440 | 143,442,792 | 2 | 185 |
| network | 168 | 5,390,218 | 1 | 7 |
| execution | 1 | 40,522,321 | 1 | global |

## Discovered physical variations

One pod file (day5/hour06) stores GPU requests, CPU requests and GPU-hours as int64 instead of double, and has null-only CPU/memory utilization. The other4,439 pod files have the usual double schemas. Server switch IDs are physically null-only in2,616files and string in1,824files. No other incompatible physical schemas or unsafe integer-to-double ranges were found.

## Source-quality observation

Day5/hour06 contains1,123,104rows with GPU requests, CPU requests and GPU-hours all zero, while GPU SM activity is sometimes positive (footer maximum1193.91) and CPU/memory utilization is null-only. This combination is retained and marked for source snapshot-consistency review. It must not be treated as proven zero demand/idle capacity. SM values are preserved; no unverified0–100clamp is applied.

The observation catalog is published under `manifests/bronze/quality-observations.json`; affected Silver commit quality reports include it. Fingerprints include only observations for their own partition. Source Parquet bytes remain unchanged.

## Per-column metadata coverage

Known null counts below come from available statistics or footer-proven null-only types. Missing statistics remain unknown. Min/max values do not establish distributions, NaN counts, duplicate keys or valid conditional relationships. IDs are opaque and are not ranked by lexical extrema. Row-level NaN/Infinity, blank/Unknown, boolean, key and join checks run in Spark. See [COLUMN_CONTRACTS.md](COLUMN_CONTRACTS.md).

### pod

| Column | Canonical type | Physical file types | Known null rows | Missing-statistics row groups |
|---|---|---|---:|---:|
| `pod_id` | string | string: 4,440 | 0 | 0 |
| `workload_id` | string | string: 4,440 | 2,513,855,734 | 0 |
| `server_id` | string | string: 4,440 | 0 | 0 |
| `cluster_id` | string | string: 4,440 | 0 | 0 |
| `state_public` | string | string: 4,440 | 0 | 0 |
| `gpu_spec_public` | string | string: 4,440 | 0 | 0 |
| `server_gpu_count` | bigint | bigint: 4,440 | 0 | 0 |
| `server_cpu_capacity_cores` | bigint | bigint: 4,440 | 0 | 0 |
| `priority_class` | string | string: 4,440 | 0 | 0 |
| `job_type_public` | string | string: 4,440 | 0 | 0 |
| `model_type_public` | string | string: 4,440 | 0 | 0 |
| `is_genai_request` | boolean | boolean: 4,440 | 0 | 0 |
| `gpu_request` | double | bigint: 1, double: 4,439 | 0 | 0 |
| `gpu_mem_request` | bigint | bigint: 4,440 | 0 | 0 |
| `cpu_request_cores` | double | bigint: 1, double: 4,439 | 0 | 0 |
| `used_gpu_hours` | double | bigint: 1, double: 4,439 | 0 | 0 |
| `avg_gpu_sm_util` | double | double: 4,440 | 6,178,628,689 | 0 |
| `avg_gpu_mem_gib` | double | double: 4,440 | 6,071,817,701 | 0 |
| `avg_cpu_request_util` | double | double: 4,439, null: 1 | 201,562,834 | 0 |
| `avg_memory_util` | double | double: 4,439, null: 1 | 300,210,925 | 0 |
| `ready_status` | boolean | boolean: 4,440 | 635,276,266 | 0 |
| `schedule_delay_sec` | bigint | bigint: 4,440 | 635,276,266 | 0 |
| `ready_delay_sec` | bigint | bigint: 4,440 | 635,276,266 | 0 |

### server

| Column | Canonical type | Physical file types | Known null rows | Missing-statistics row groups |
|---|---|---|---:|---:|
| `server_id` | string | string: 4,440 | 0 | 0 |
| `cluster_id` | string | string: 4,440 | 0 | 0 |
| `asw_id` | string | null: 2,616, string: 1,824 | 79,031,064 | 0 |
| `gpu_spec_public` | string | string: 4,440 | 0 | 0 |
| `gpu_count` | bigint | bigint: 4,440 | 0 | 0 |
| `cpu_capacity_cores` | bigint | bigint: 4,440 | 0 | 0 |

### network

| Column | Canonical type | Physical file types | Known null rows | Missing-statistics row groups |
|---|---|---|---:|---:|
| `server_id` | string | string: 168 | 0 | 0 |
| `rx_gibps_avg` | double | double: 168 | 0 | 0 |
| `tx_gibps_avg` | double | double: 168 | 0 | 0 |

### execution

| Column | Canonical type | Physical file types | Known null rows | Missing-statistics row groups |
|---|---|---|---:|---:|
| `pod_id` | string | string: 1 | 0 | 0 |
| `workload_id` | string | string: 1 | 35,729,919 | 0 |
| `server_id` | string | string: 1 | 0 | 0 |
| `gpu_spec_public` | string | string: 1 | 0 | 0 |
| `priority_class` | string | string: 1 | 0 | 0 |
| `job_type_public` | string | string: 1 | 0 | 0 |
| `model_type_public` | string | string: 1 | 0 | 0 |
| `is_genai_request` | boolean | boolean: 1 | 0 | 0 |
| `gpu_request` | double | double: 1 | 0 | 0 |
| `duration_hours` | double | double: 1 | 0 | 0 |
| `schedule_delay_sec` | bigint | bigint: 1 | 392,427 | 0 |
| `ready_delay_sec` | bigint | bigint: 1 | 392,427 | 0 |
| `ready_status` | boolean | boolean: 1 | 392,427 | 0 |
| `schedule_status` | boolean | boolean: 1 | 392,427 | 0 |

Full extrema and schema signatures are retained in [docs/whole-bronze-preflight.json](whole-bronze-preflight.json).
