# Gold transformation plan — capacity planning

Design updated November20,2026. The user authorized Gold execution. `gold.py`, `dags/gpu_gold.py` and guarded operators are implemented, locally tested and deployed. The same-worker handoff is armed after verified Silver success. Legacy pilot Gold is a separate product.

Implementation checkpoint: the first release produces hourly capacity/activity/mix/quality evidence, daily placement bins, aligned network context, global execution aggregates and a checksummed offline Parquet bundle. Server-hour intermediates are cached within each Spark application rather than written to S3. Historical-window scenario UI, ranked opportunity rules, additional telemetry histograms and expanded distribution metrics remain follow-up work; do not claim those dashboard features already exist. See the [Gold runbook](../runbooks/GOLD_RUNBOOK.md).

## Decision and scope

Use one existing m6a.2xlarge, Spark for aggregation and Airflow for bounded day-level tasks. Read each large Silver pod day once, write reusable reduced facts, and build window summaries from those facts. Keep one Spark application active at a time with the tested six threads/12-GiB heap and disk/runtime guards. No second machine is justified before measuring a Gold pilot. The authorized handoff continues on the same worker only after verified Silver success, with an explicitly tested finalizer wait and independent Gold shutdown guards. Any Silver failure retains the original shutdown path.

Main planning data covers relative days0–184; no calendar dates are invented. Network is separate context for days109–115 only. Execution is global and must not be joined into hourly demand using pod ID alone. GPU model labels are masked cohorts, not interchangeable performance units.

## Entry gate

Require verified completion of all378 Silver partitions and192 final inventory audits. Resolve accepted paths through current manifests; pin the complete source manifest set and code/configuration hashes for this Gold release. Read Silver quality reports and catalog source observations before accepting derived metrics. If a source changes mid-run, fail or start a new release; do not mix snapshots. Check actual duplicate candidate counts and lifecycle vocabularies before activating demand/scenario metrics.

## Products and grains

| Product | Grain | Main fields and purpose |
|---|---|---|
| server_hour_evidence (intermediate, S3) | day/hour/server | Inventory model/cluster, valid GPU/CPU capacity, pod observation count, valid request sums/counts, unknown/invalid request counts, valid GPU-time sum, telemetry sums/counts, model/cluster mismatch counts, quality indicators. Retain inventory-only rows with unknown demand. |
| capacity_hourly | day/hour/inventory model/cluster | Capacity counted once per valid inventory server; matched observed request sum, request-evidence coverage, unmatched/mismatched demand separately, source-quality status. Full small hourly series powers selectable windows. |
| workload_mix_hourly | day/hour/model/cluster/one named dimension/value | Request/activity components by priority, job type, model type, GenAI three-state, or lifecycle label. Separate dimension families avoid a combinatorial cube. Capacity remains in capacity_hourly and is not summed across mix rows. |
| activity_evidence_hourly | day/hour/model/cluster | Finite source-unit SM sums/counts and fixed histogram bins; valid memory/CPU measurement counts/sums; GPU-requesting row and request-weight coverage. Never label a pod-row mean as fleet utilization. |
| placement_daily | day/model/cluster | Fixed server request-pressure bins, over-requested server-hour counts, observed headroom distribution, inventory-only/missing-demand counts; bounded server investigation examples. |
| opportunity_evidence | cohort/review window/rule version | Persistence, eligible versus excluded hours, low-activity/request mismatch evidence, measurement coverage, CPU/memory context and reasons for review. Rule thresholds explicit; no guaranteed reclaimed GPU count. |
| planning_quality | dataset/day/model/cluster/check | Source anomalies, field missingness/invalidity, candidate duplicates, quarantine, join mismatch/unmatched counts and metric eligibility. Detailed metadata alongside every chart. |
| network_context_daily | day/model/cluster, days109–115 | RX/TX finite sums/counts and server-hour coverage with unmatched inventory diagnostics. Not link saturation; link capacities are unknown. |
| execution_context_global | model/priority/job type | Observed span distributions, valid delay distributions, status counts and coverage. Independent global page; no daily filter or daily capacity attribution. |
| planning scenarios (computed locally) | selected cohort/window + explicit assumptions | Concurrent observed request anchor, capacity reference, coverage and user growth/reserve assumptions. Derived from hourly Gold; no AWS dependency. |

Do not export the full server-hour intermediate to Streamlit. Export all compact hourly/cohort products, histograms and bounded examples. Exact arbitrary-window distinct workloads or percentiles are unsupported unless materialized with a suitable mergeable representation; do not sum daily distincts or average daily percentiles.

## Core transformation rules

1. Aggregate pod observations to server/day/hour before joining unique inventory. Validate join row counts and capacity totals. Preserve unmatched pod aggregates separately and expose model/cluster mismatches; authoritative capacity cohorts come from inventory. Do not silently allocate mismatched demand to an invented cohort.
2. Valid request components are finite and nonnegative; zero and fractional requests remain valid. CPU-only observations stay in CPU context. Record excluded/unknown values separately. Resolve repeated pod/server/hour candidates before treating summed requests as concurrent demand: only safe identical duplicates may be collapsed under a reviewed rule; conflicting candidates remain flagged and suppress unsupported demand/scenario conclusions. Preserve Silver unchanged.
3. Sum valid requests across eligible observations within the same hour/cohort, then compute historical peaks/p95 over that hourly series. This is an hourly observed request proxy, not proven instantaneous peak or unmet demand. Publish raw lifecycle-label splits; do not infer running from unknown labels. Lifecycle eligibility requires an explicit reviewed mapping.
4. Request-to-capacity = eligible observed requests / valid unique inventory GPU capacity for the same cohort/hour. Zero/null capacity gives null ratio with reason. Ratios above1 remain visible. Unobserved servers have unknown demand, not idle GPUs. Matched-server coverage is an observability indicator, not proof that all pods on those servers are observed. Partial sums must be labeled as partial, and scenario readiness must remain false where material completeness is unestablished.
5. Observed headroom = inventory capacity minus eligible observed requests on sufficiently evidenced servers. Preserve signed values; expose excess requests separately. This is a request proxy, not schedulable/device-level fragmentation or safely reclaimable capacity. Do not add unknown headroom as free capacity.
6. Preserve used_gpu_hours as reported attribution; validate finite/nonnegative inputs and report disagreements with request counts. Do not force equality with requests or infer an hourly slot utilization ratio without confirmed source semantics.
7. SM values stay in source units, including values above100; do not clamp or call them percentages. Compute unweighted observation means from sums/counts; any request-weighted view must be separately named and is not measured GPU-weighted fleet utilization. Keep missing measurements explicit. CPU request utilization above1 is retained; utilization/memory ratios require confirmed compatible units and denominators.
8. Day5/hour06 is source-inconsistent: retain raw derived components and flag the hour. Default decision-grade demand/efficiency anchors exclude it, with an inclusion toggle and excluded-hour count. Do not interpret its zero requests/positive GPU activity as zero demand. Generalize this to the versioned source-observation catalog.
9. Negative/sentinel delays are excluded from nonnegative delay summaries with excluded counts; true/false/unknown status is separate. Observed execution span does not prove completed runtime. Missing workload IDs must not be grouped into one real workload. No execution-to-hourly many-to-many join.
10. Unknown category values remain explicit. No model performance equivalence, cost comparison or purchase recommendation is inferred from masked names. All aggregations carry sufficient numerator/count components to recompute weighted summaries correctly.

## Dashboard outcomes

- Capacity outlook: hourly observed requests versus inventory by compatible cohort, trends and sustained request pressure, with unknown/mismatched/excluded observations alongside the chart.
- Efficiency review: persistent request/activity mismatches ranked as investigations, showing supporting duration and telemetry coverage rather than a reclaim total.
- Placement/mix: uneven request pressure and hardware mix, bounded host examples, explicit unknown coverage and server-level proxy labeling.
- Planning scenarios: choose observed p95/peak anchor, explicit capacity reference (latest eligible hour or conservative selected minimum), growth>=0 and0<=reserve<1. Illustrative required_slots=ceil(anchor*(1+growth)/(1-reserve)); additional_slots=max(0,required_slots-capacity_reference). Label slots within the masked cohort, not equivalent GPU performance or an order quantity. Suppress definitive recommendation where coverage/semantics gates fail. Optional prices must be supplied separately.
- Network/global execution are clearly scoped supporting tabs; changing the main date window must not relabel their coverage.

Thresholds for sustained pressure, low activity, persistence and coverage are versioned configurable assumptions, not hard-coded findings. Report sensitivity to threshold changes. Default first release shows evidence and marks procurement scenarios exploratory until semantic/coverage gates are reviewed.

## Airflow, publication and offline handoff

New manual Gold-only DAG: validate pinned Silver release -> mapped daily reduction (serial initially) -> network/global context -> reduced-series/window summaries -> QA -> offline bundle -> publish release manifest. Gold cannot start from partial Silver or invoke legacy pilot DAGs. Day retries create immutable attempts and atomically replace only their manifest pointers; the release manifest is published last after complete validation. Every output includes source release/config/code identity. A failed retry must leave the prior published release readable.

Write versioned Parquet Gold in S3. Build a portable local bundle with compact Parquet/JSON, schemas, metric definitions, quality annotations, source window/cohort metadata, hashes and software versions. Streamlit reads local files without boto3 calls or AWS authentication. Target initial bundle under100MiB, measure actual size, and trim bounded examples rather than discard needed quality metadata. No claim that this target is already met.

Download and verify bundle hashes, start Streamlit with AWS access disabled, and compare displayed metrics to independent aggregates before any teardown. Keep a release archive outside AWS. Gold execution is authorized; cloud deletion remains a separate instruction.

## Validation and pilot

Meaningful tests: inventory fanout cannot multiply capacity; missing telemetry stays unknown; fractional/zero/NaN/Infinity requests; duplicate/conflicting pod candidates; unknown lifecycle/GenAI; model/cluster mismatches; zero capacity; requests above capacity; anomalous source hour; percentile computed from concurrent hourly totals; rerun replacement leaves neighboring days intact; failed publication leaves old release; network/global scopes never expand with the main filter; offline dashboard works without AWS.

Pilot days3,5,109,173 cover early/rare schema/anomaly/aligned-network/larger workloads. Validate server-hour and cohort capacity against independent Silver sums; reconcile source -> valid/excluded/unmatched populations; enforce one row per published grain; compare sampled aggregates independently with Arrow/DuckDB where practical. Record Spark elapsed time, memory, disk minimum, input bytes, output sizes and reuse of daily reductions. Then choose the full run budget and optional tuning. Benchmark only after Silver completes; never run alongside it.

Gold is aggregation-heavy but must still scan billions of Silver rows. No Gold benchmark exists, so a duration promise would be misleading. Initial budget envelope for planning:4–12hours on the existing worker, explicitly unmeasured and revisable after the four-day pilot. At the project's previously verified compute rate0.3456USD/hour, that corresponds to1.38–4.15USD compute only; refresh prices before execution and add retained S3/EBS, requests, IPv4 and local bundle egress. Do not treat credits as a hard spend cap. Stop compute after evidence/export preservation, with bounded independent shutdown protection.

## Next review decisions

Review the table/metric contracts and source semantics first; validate complete Silver quality reports; implement and locally test Gold; run the bounded four-day pilot; review measured runtime/size and decision eligibility; then launch full Gold. Gold execution and the guarded same-worker handoff were authorized November20. The four-day pilot must pass the12-hour projected daily-processing gate before the remaining185-day scope proceeds. No additional approval is required within that authorized scope.
