# Silver implementation and benchmark gate

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

Completed run: `silver_full_20261119_r2`, 378 transformation partitions, 192 audit bindings and 6,759,480,015 accepted rows. See the [saved AWS evidence](../evidence/README.md). Benchmark sections below are dated historical measurements. `pipelines/pipeline.py`, `gpu_trace_day` and `gpu_medallion_backfill` are legacy pilot entry points that also create pilot Gold; use the Silver DAG for full-width processing.

## What is implemented

- `pipelines/silver.py`: full-width schemas for four tables; checks every physical Parquet footer against its contract, verified Bronze key/size inventory and 24-hour coverage, then reads S3 through Spark S3A. All original fields retained. Day/hour are integer source partitions; execution stays global. CPU-only pods and fractional requests are preserved. No blind deduplication or unit conversion.
- Quality: per-column null counts, per-value negative/non-finite/unknown flags, negative-delay flags, nullable GPU-requesting flag, finite SM presence flag. Negative delay raw values remain untouched; sentinel meanings are unconfirmed. NaN/Infinity remain raw, with flags; downstream analysis must use quality predicates.
- Quarantine: invalid hour, missing server key on inventory/network, ALL rows sharing a duplicate server-hour key on inventory/network. Workload/execution candidate-key repetitions are retained and counted because their uniqueness is unproven. Ordinary nulls and unknown categories remain accepted. No arbitrary duplicate winner is chosen.
- Per-partition input = accepted + quarantined reconciliation, Spark read-back counts and independent PyArrow footer counts. Schema/coverage/inventory drift, corrupt input, empty input or count mismatch fail publication. Warnings/quarantine counts are review evidence, not proof a dataset is safe for buying decisions.
- Inventory matching uses semi/anti joins on day/hour/server; it reports observation coverage and inventory keys without observations, without repeating capacity. Final Airflow audits resolve current inventory after mapped tasks complete. Missing compatible server Silver fails that final audit. No matching inventory observation is unknown, not zero capacity/demand.
- `dags/gpu_silver.py`: manual-only `gpu_silver_backfill`, one mapped task per dataset/day, one active task/run, retry once, followed by inventory audits. It invokes only Silver. Same-partition manual/concurrent writers are unsupported. PostgreSQL/LocalExecutor production services have NOT been configured or validated by the local `dags test` run.

## Publication, restart and reading

Outputs: `silver_versions/{dataset}/{day=N|all}/silver-v1/{attempt}/{accepted|quarantine}/` as ZSTD Parquet. A report is written inside the immutable attempt; `manifests/silver/{dataset}/{day=N|all}.current.json` is published LAST. Failed attempts never replace the pointer. The manifest contains source fingerprint, transformation code hash/version, output inventories, counts and Spark application ID. Matching completed source+code+output inventories are skipped on resume. A forced rerun commits a different attempt; consumers resolve exactly ONE current manifest and read its `accepted` path. NEVER wildcard all `silver_versions` attempts. The GPU workload view is `is_gpu_requesting = true` on committed accepted pod data.

Final join audits live in `manifests/silver_audits/{dataset}/day=N.json`, bound to a committed Silver attempt and inventory source fingerprint. Reaudit if either source changes. Current manifests are the partition ledger; failed Airflow task state/logs provide failure history. There is no cross-partition transaction or concurrent-writer lock. An EC2 root loss does not lose committed S3 outputs, but orchestration metadata must be backed up before teardown.

Old/failed attempts are retained for inspection and rollback. Automated garbage collection is NOT implemented; review/delete only unreferenced attempts after successful verification and retention review. This adds storage on forced benchmarks/reruns. An output-inventory match does not cryptographically rehash every remote byte. Bronze verification remains tied to the completed ingestion evidence plus current key/size/ETag fingerprints.

## Reproduce locally (Python 3.11, Java 17)

Use the existing pinned project environment, with `SPARK_HOME` unset and `JAVA_HOME` pointing at Java 17. Install requirements plus Airflow 3.0.6 with its official Python 3.11 constraints, as in the CI workflow. No AWS credentials are required for local validation.

```bash
python -m unittest discover -s tests -v
python examples/make_silver_sample.py /tmp/gpu-silver-sample
export TRACE_PROJECT="$PWD"
export TRACE_SILVER_ROOT=/tmp/gpu-silver-sample
export AIRFLOW_HOME=/tmp/gpu-silver-airflow
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export SPARK_LOCAL_IP=127.0.0.1
airflow db migrate
airflow dags test gpu_silver_backfill 2026-11-19 \
  -c '{"datasets":["server","pod","network","execution"],"days":[109]}'
```

GitHub Actions now includes this Silver DAG test. Local tests include full-width field preservation, CPU-only rows, fractional/null GPU requests, null/zero/NaN SM, CPU ratios above one, raw negative delays, duplicate-key quarantine, Arrow counts, joins, resume, forced rerun, injected commit failure, missing source objects and schema drift. The actual local Airflow test completed all four transforms plus final audits. Local Airflow uses an isolated SQLite pilot database; this does not certify a production scheduler deployment.

## Real-data benchmark — completed configuration

The completed benchmark used m6a.2xlarge (8 vCPU / 32 GiB), six Spark threads and 12 GiB driver heap after m6i capacity was unavailable. The root started at30GiB and was expanded with user approval to60GiB after a safe scratch-space stop. The runner stops its process group below10GiB free or after90minutes; its enclosing service caps total runtime at100minutes. Maintain the existing two-hour EC2 boot stop timer as an outer pilot safety guard; verify its behavior before boot. The runner alone does not stop EC2; the reviewed benchmark systemd service finalizer saves logs and stops it. Always verify actual EC2 state after collecting evidence. Do not deploy PostgreSQL production services until the topology is decided.

Cases: server days 3/109/173, pod days 3/109/173 at six threads, pod109 again at three threads, network109 and global execution. Four final audits cover those workload days and network109. Each attempt is isolated and requires completed Bronze manifests. The smaller/larger workload days bracket known input sizes. The three-thread comparison measures CPU sensitivity on the SAME machine; it is not a measured m6i.xlarge or two-machine result.

```bash
python examples/benchmark_silver.py \
  --root s3://gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6 \
  --results /opt/gpu-fleet/logs/silver-benchmark \
  --threads 6 --driver-memory 12g --max-seconds 5400 --min-free-gib 10
python examples/estimate_silver.py /opt/gpu-fleet/logs/silver-benchmark/results.json
```

Run only on a prepared, explicitly selected benchmark worker with the existing IAM role and pinned dependencies; never on the laptop against all real data. Save logs, Spark event logs and `results.json` to S3 and locally before stopping. Monitor whole-process-group RSS, minimum disk free, wall time including startup, executor CPU/run/GC time, spill, input/output bytes, file sizes, counts and join coverage. Inspect GC/heap/disk pressure before extrapolating. Timeouts/failures invalidate the projection and require review; don't automatically expand storage or start more machines.

## Decide one machine versus parallel machines before full run

| Option | What evidence is needed | Tradeoff |
|---|---|---|
| One 8 vCPU / 32 GiB worker | Actual representative-day throughput, memory/spill and disk headroom; projected completion within target | Simplest orchestration and recovery; initial preference if it meets the deadline |
| Two 4 vCPU / 16 GiB workers, independent days | Actual smaller-host fit and concurrent-job benchmark; account for control-plane resources and partition skew | Parallel days may finish faster, but half the memory per job and extra deployment/coordination |
| Two 8 vCPU / 32 GiB workers | Actual two-worker concurrency benchmark; evidence that shorter elapsed time is worth roughly twice the hourly compute | Stronger parallel capacity; S3/disk/CPU scaling is not assumed linear |

Airflow controls job scheduling; Spark parallelizes transformations. Two EC2 workers running independent per-day Spark applications are distinct from a distributed Spark cluster. LocalExecutor cannot remotely schedule a second worker by just increasing task concurrency; that deployment would need a selected remote mechanism/executor and separate validation. No distributed cluster, extra worker, or full backfill is provisioned by these scripts.

`estimate_silver.py` uses measured wall-time/input-byte rates by dataset, observed startup floors and 25% retry allowance. It outputs scenarios, not a statistical confidence interval. Its two-worker half-time figure is explicitly an ideal lower-bound comparison, not a forecast. It requires all four dataset benchmarks; it cannot turn synthetic timings into a real-data estimate. The completed reports supersede the old8–24-hour allowance with a measured16–25-hour Silver scenario, including final audits and upper-scenario retry allowance. Gold is excluded and still requires separate implementation/review.

Budget context: previously checked Ohio compute rates were $0.384/hour for m6i.2xlarge and $0.192/hour for m6i.xlarge; refresh before launching. At those rates a 90-minute single-host pilot is ~$0.58 compute, plus startup time outside the runner, IPv4, storage and requests. It is not an enforced billing cap. User-provided management-account evidence showed $153.53 estimated remaining credits from $160 issued, expiration November 2, 2027; all four credits list EC2/S3/VPC/data transfer, and user confirmed project-account sharing activated. Balance can lag and is shared with other organization usage.

Before the full run: show measured per-option duration/cost/storage scenarios to the user, finalize topology, configure and validate persistent Airflow/PostgreSQL if using that deployment, configure a suitable runtime stop policy, then obtain the user's full-run decision. No full backfill is authorized yet.

## Authorized benchmark in progress — November 19

User approved resizing/starting the existing worker for the benchmark only. `m6i.2xlarge` could not start in Ohio 2a (`InsufficientInstanceCapacity`). The equivalent 8-vCPU/32-GiB x86 worker `m6a.2xlarge` started successfully at a refreshed AWS Pricing API rate of $0.3456/hour, lower than $0.384/hour for m6i.2xlarge. No second worker or extra EBS volume was provisioned. This direct instance-type change creates drift from the historical CloudFormation template's t3.large; reconcile that template before any future stack update.

Preflight: 26 GiB free on the existing 30-GiB root, 30 GiB RAM available, 8 vCPU, Java 17, PySpark 3.5.5, PyArrow 26.0.1, Boto3 1.39.11, pip check passed. The two-hour boot stop timer is active. `gpu-silver-benchmark.service` limits the entire job to 100 minutes; the Spark runner has a 90-minute deadline and 10-GiB minimum-free guard. Its root finalizer uploads logs and shuts down EC2 on success or failure. The service is not enabled for future boots. Reusable operator scripts are in `infra/scripts/silver-benchmark-*.sh` (substitute the bucket placeholder before using).

Cloud Airflow smoke on server day 3 initially failed a strict schema check: `asw_id` has Parquet Null logical type for null-only data in that partition. No Silver was published on the failed attempt. The reader now groups files by footer-proven null-only fields, prunes those physical fields while reading, and restores nulls with the declared logical types. It still rejects populated incompatible types and missing/extra columns. Local regression now covers all-null and mixed null/string files; all seven tests pass. The updated reader passed the Airflow cloud retry on server day 3. The benchmark proceeds only over the approved cases; no full backfill or new Gold is scheduled.

## First benchmark attempt outcome — worker stopped

The benchmark stopped safely on pod day 173 at 9.42 GiB free, below the 10-GiB threshold. Automatic finalization preserved logs under `run-logs/silver-benchmark-20261119/` and EC2 was verified stopped. A read-only S3 check confirmed no committed `manifests/silver/pod/day=173.current.json` exists. Successful cases remain committed; the incomplete attempt is unreferenced. The original Bronze and legacy Gold were not rewritten. Scratch cleanup restored about 26 GiB free; persistent disk exhaustion was avoided.

Completed cases: server3 30.2s/676,080 rows; server109 28.2s/775,104 rows; server173 28.2s/888,024 rows; pod3 229.1s/25,777,119 rows; pod109 277.4s/36,264,315 rows. Peak measured process-group RSS was 13.17 GiB for the completed workloads. Pod3 and pod109 had no quarantined rows and 0 unmatched inventory observations. These are partial timings, not a completed full-run forecast. Largest day, the three-thread CPU comparison, network, global execution and final audits still need completion.

A proposed root-only expansion from 30 to 60 GiB would add $2.40/month (about $0.08/day) at the refreshed Ohio gp3 rate of $0.08/GiB-month. Keep 3,000 IOPS and 125 MiB/s unchanged for comparability. No expansion has been executed; user approval is pending. No 500-GiB disk or second EC2 is proposed. `--resume` now validates transformation code hash, source fingerprint, driver memory and exact immutable output inventories before reusing each completed timing. A local regression check confirms it refuses changed code, source markers, memory settings and truncated output files. It measures final audit durations explicitly; the estimator includes those costs and refuses incomplete benchmarks. Failure logs are retained on resume. Prepare the refreshed bundle, grow the filesystem after an approved expansion, rerun preflight, resume missing cases, preserve evidence and verify EC2 stopped again. Full backfill remains unapproved.

## Second benchmark session — in progress on approved 60-GiB root

The user approved the proposed expansion and resume. Root volume `vol-0aa7b9d7b5285efac` was modified to 60 GiB on November 19 at 17:02 Pacific, preserving 3,000 IOPS and 125 MiB/s. The same stopped m6a.2xlarge was restarted; its XFS filesystem is now ~59.93 GiB with ~54.96 GiB free before work. Five saved case reports passed code/source/output validation and were reused without rerunning. The largest workload day173 now passes: 323.6 seconds for 43,216,910 rows, 13.13 GiB peak process RSS, 6.147 GiB aggregate Spark disk spill and 38.59 GiB minimum free disk. Remaining CPU comparison, network, execution and final audits are running. The new runner explicitly records final audit timings for the forecast. The two-hour boot timer and 100-minute benchmark service guard remain active. Do not launch a full backfill or another benchmark concurrently. Finalizer uploads logs and stops EC2; verify stopped state after completion. Both CPU type and root size now differ from the historical CloudFormation template, so reconcile desired state before any stack update.

## Final verified outcome — benchmark complete, EC2 stopped

All 9 cases and 4 final audits succeeded; service-result.json records successful exit 0 and EC2 is verified stopped. Five saved measurements were validated/reused. The recommended measured baseline is one m6a.2xlarge with the approved 60-GiB root, six Spark threads and 12-GiB driver heap. Forecast: 16–25 hours for Silver including final audits and upper-scenario retry allowance, roughly $7–$11 before credits with short retention. This excludes new Gold and persistent Airflow/PostgreSQL deployment. Eight unique Silver partitions are committed (370 remain). The three-thread comparison took 363.3s versus 277.4s at six threads; smaller-machine and multi-machine performance remain unmeasured. Network109 retains 190 unmatched inventory observations for explicit quality reporting. Read [AWS_BENCHMARK_RESULTS.md](../evidence/AWS_BENCHMARK_RESULTS.md) before any full-run decision. The user has not authorized a full backfill. Earlier in-progress and failed-attempt sections are history, superseded by this final outcome.
