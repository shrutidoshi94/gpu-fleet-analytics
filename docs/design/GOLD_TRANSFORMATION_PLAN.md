# Gold design

## Audience

Capacity planners investigating request pressure and whether existing infrastructure deserves closer review. The trace contains requests and sampled activity; it does not establish complete simultaneous demand, interchangeable GPU performance or safe purchase/reclaim quantities.

## Tables

| Product | Grain | Use |
|---|---|---|
| capacity_hourly | day, hour, GPU type, cluster | Recorded inventory and observed request pressure |
| activity_evidence_hourly | same + match flags | Activity sums, valid counts and coverage |
| workload_mix_hourly | same + dimension/value | Separate request-weighted workload marginals |
| placement_daily | day, GPU type, cluster, headroom bin | Distribution of server-hour request headroom |
| planning_quality | day, hour, GPU type, cluster, match flags | Workload/inventory matching and request validity |
| network_context_hourly | day, hour, GPU type, cluster | Network context for days 109–115 only |
| execution_context_global | GPU type, priority, job type | Execution-span context without a daily mapping |

Exact fields and legal aggregations: [Gold dictionary](../data/GOLD_DATA_DICTIONARY.md).

## Calculation decisions

Reduce workloads by server-hour before joining inventory; count capacity once per inventory record. Use valid-observation counts to combine activity means. Compute request percentiles from summed hourly observations rather than averaging subgroup percentiles. Requests within an hour are not proven simultaneous.

Keep source anomalies, unmatched observations and repeated candidate keys explicit. Repeated candidate keys remain unresolved; the conservative screen excludes their whole day. This removes 53 of 185 days in the full release. The dashboard shows screened versus unscreened p95 and evidence hours so that the selection effect is inspectable. Headroom unknowns can also arise from absent observations, invalid request/capacity values or incompatible labels.

The growth model is `ceil(request_anchor × (1 + growth) / (1 − reserve))`, using one GPU type and an explicit inventory reference. It is a sensitivity calculation, not a trained forecast. No cost-saving or purchase quantity is asserted.

## Execution and portability

One EC2 worker ran one Spark application at a time. Airflow handled ordering/retries, while Spark parallelized within the machine. A four-day pilot estimated the full Gold runtime before the remaining reductions. The completed run took 8h52m; its seven-table portable export met the 100-MiB design target at about 47.4 MB.

Pinned source manifests, immutable attempts and final publication checks preserve provenance. Historical manifests reference the original deployed source; refactoring creates a new code identity without rewriting those records. The original sources are retained under `docs/evidence/deployed-source/`.

The dashboard downloads a pinned GitHub Release asset when needed and reads local files afterward. No S3 request or AWS credential is needed for visitor interactions. Network data remains limited to 109–115; execution context is never relabeled as a selected-day measurement.
