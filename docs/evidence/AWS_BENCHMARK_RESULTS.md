# Verified AWS Silver benchmark and machine recommendation

Completed November 19, 2026. **EC2 is verified stopped. All 9 benchmark cases and 4 final join audits completed successfully. No full backfill or new Gold generation has started.**

The tested worker is m6a.2xlarge: 8 vCPU / 32 GiB, six Spark threads, 12-GiB driver heap, gp3 root at 3,000 IOPS / 125 MiB/s. m6i.2xlarge had insufficient capacity in Ohio 2a; the equivalent AMD worker was available at a lower verified price.

## What blocked the first attempt

The 30-GiB root initially had about 26 GiB free. Spark caches intermediate frames on disk and writes shuffle/spill files while validating and repartitioning the full-width data. The largest workload day crossed the 10-GiB safety threshold at 9.42 GiB free. The guard stopped the job, preserved logs and shut down EC2; it did not commit the partial partition.

After explicit user approval, the same root was expanded to 60 GiB and its XFS filesystem verified. CPU, RAM, IOPS and throughput stayed unchanged. Five previously completed cases passed source/code/output checks and were reused. The largest day then passed with 38.59 GiB minimum free, about 13.13 GiB peak process RSS and 6.15 GiB aggregate Spark disk spill. No 500-GiB disk or second worker was provisioned.

An earlier smoke-test schema rejection also exposed a legitimate source edge case: `asw_id` is physically null-only in early inventory data. The reader now prunes only footer-proven null-only physical columns and restores typed nulls. Populated incompatible types still fail. All-null and mixed-null/string regression tests passed, then the cloud Airflow smoke test passed.

## Actual measurements

| Dataset / day | Spark threads | Rows in = accepted | Wall time | Peak process RSS | Root capacity / minimum free |
|---|---:|---:|---:|---:|---:|
| server / 3 | 6 | 676,080 | 0m 30.2s | 2.76 GiB | 30 / 24.82 GiB |
| server / 109 | 6 | 775,104 | 0m 28.2s | 3.12 GiB | 30 / 24.75 GiB |
| server / 173 | 6 | 888,024 | 0m 28.2s | 2.88 GiB | 30 / 24.72 GiB |
| pod / 3 | 6 | 25,777,119 | 3m 49.1s | 13.15 GiB | 30 / 14.53 GiB |
| pod / 109 | 6 | 36,264,315 | 4m 37.4s | 13.17 GiB | 30 / 12.52 GiB |
| pod / 173 | 6 | 43,216,910 | 5m 23.6s | 13.13 GiB | 60 / 38.59 GiB |
| pod / 109 | 3 | 36,264,315 | 6m 3.3s | 8.49 GiB | 60 / 42.44 GiB |
| network / 109 | 6 | 766,207 | 0m 30.2s | 3.02 GiB | 60 / 54.56 GiB |
| execution / global | 6 | 40,522,321 | 3m 49.0s | 13.10 GiB | 60 / 44.01 GiB |

Every case reconciled input = accepted + quarantined; all sampled cases had zero quarantined rows. The six-thread and three-thread day109 quality reports are identical. Committed outputs passed Spark read-back and independent PyArrow footer row-count checks.

Six threads reduced elapsed time by 23.7% compared with three threads (observed speedup 1.31x). Three threads used less memory and no Spark disk spill. These are single trials on the SAME 32-GiB host, across two boots/root capacities with unchanged IOPS/throughput; this is not a benchmark of a 16-GiB instance or multi-machine scaling.

## Final join audits

| Dataset / day | Matched observations | Unmatched observations | Inventory server-hours without observations |
|---|---:|---:|---:|
| pod / 3 | 25,777,119 | 0 | 12,442 |
| pod / 109 | 36,264,315 | 0 | 8,903 |
| pod / 173 | 43,216,910 | 0 | 5,886 |
| network / 109 | 766,017 | 190 | 9,087 |

The 190 unmatched network records (~0.025% of day109 network observations) are retained and reported; they must not be silently dropped or assigned inventory attributes. Inventory hours without observations are unknown coverage, not proven idle capacity. These findings belong in Gold/dashboard quality indicators. Each saved final audit was checked against the currently committed Silver attempt and corresponding inventory source fingerprint.

## Runtime forecast for Bronze → Silver

The measured single-worker scenario is **16.2–24.9 hours**, rounded to **16–25 hours**, for all 378 dataset/partition units plus final audits. The upper scenario includes a 25% retry allowance. Eight unique partitions are already committed, so 370 remain; this slightly reduces remaining work but does not materially change the rounded scheduling allowance.

| Work | Lower scenario | Upper scenario |
|---|---:|---:|
| pod transforms | 13.46 h | 20.20 h |
| server transforms | 1.19 h | 2.33 h |
| network transforms | 0.06 h | 0.07 h |
| execution transforms | 0.06 h | 0.08 h |
| pod final audits | 1.44 h | 2.19 h |
| network final audits | 0.03 h | 0.04 h |

This is a throughput scenario, not a confidence interval or completion guarantee. It extrapolates observed per-byte wall times across small/reference/large workload days and inventory days, adds fixed-startup floors, the measured final-audit range and retry allowance. Persistent Airflow/PostgreSQL setup, its cohosted resource overhead, unforeseen drift/quality failures, later Gold implementation/processing and offline-dashboard work are outside this forecast. Production scheduler deployment is not certified by `airflow dags test`.

## Cost before credits

Compute at the refreshed Ohio m6a.2xlarge rate ($0.3456/hour) is **$5.61–$8.61**. Public IPv4 adds approximately $0.08–$0.12. The root is now $4.80/month (~$0.16/day), an increase of ~$0.08/day from 30 GiB.

Observed output/input ratios project approximately **283–350 GiB of new Silver** for the whole trace. Including existing data, retained older attempts/pilots and subtracting already committed new Silver avoids double counting; projected total S3 storage is approximately 619–686 GiB. At $0.023/GiB-month, two days of that S3 storage plus the 60-GiB root is approximately **$1.27–$1.37**.

Use **roughly $7–$11 before credits** as the planning allowance for Silver compute, public IP, short retention and small request/startup overhead. This is not an enforced cap; Gold, taxes and longer retention are excluded. Old/failed attempts remain until retention cleanup is reviewed. The user confirmed project-account credit sharing and provided service lists covering EC2/S3/VPC/data transfer; the management dashboard last showed $153.53 estimated remaining, which can lag and is shared with other organization usage.

## Machine choice before the full run

| Option | Compute rate | Evidence and decision |
|---|---:|---|
| **One m6a.2xlarge, 8 vCPU / 32 GiB, 60-GiB root** | **$0.3456/h** | **Recommended tested baseline. 16–25-hour Silver scenario; simplest orchestration and recovery.** |
| Two m6a.xlarge, each 4 vCPU / 16 GiB | $0.3456/h combined | Same combined CPU/RAM and hourly rate, but different memory/page-cache/I/O behavior. A 3-thread test on 32 GiB cannot certify smaller-host fit or concurrent throughput. Benchmark those conditions before choosing. |
| Two m6a.2xlarge, each 8 vCPU / 32 GiB | $0.6912/h combined | Ideal half-time arithmetic is ~8–12.5h; **not a measured forecast**. Requires remote job scheduling, actual concurrency validation and handling skew/shared bottlenecks. Total compute cost need not double if elapsed time falls proportionally. |
| One m6a.4xlarge, 16 vCPU / 64 GiB | $0.6912/h | Not benchmarked. More CPU/RAM does not fix scratch-space requirements or guarantee twice the throughput. Tune and benchmark before adopting. |

For the stated two-day window, retain the tested single-worker option for the first backfill unless a shorter deadline justifies another topology benchmark. Spark runs locally across cores on one EC2; this is not a multi-node Spark cluster. Airflow controls partition scheduling. Multiple machines would preferably process independent days; they are not enabled by simply increasing LocalExecutor concurrency.

## Handoff and next boundary

- EC2 `i-09dc0bfe45b0d275f`: stopped, m6a.2xlarge. Root `vol-0aa7b9d7b5285efac`: 60-GiB gp3, 3,000 IOPS / 125 MiB/s. No 500-GiB disk or second worker.
- Evidence: `s3://gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6/run-logs/silver-benchmark-20261119/`, committed Silver manifests and `manifests/silver_audits/`. Local copies and final machine state are saved in the chat workspace; the structured report accompanies this file.
- Read only the accepted path selected by each current Silver manifest. Do not wildcard historical attempts or consume canceled legacy day109 pilot output. The new v1 day109 Silver is validated.
- Before a full run: finalize the machine choice, configure and validate persistent Airflow/LocalExecutor with separate PostgreSQL metadata, preserve SQLite pilot history, set six Spark threads/12-GiB heap explicitly, and replace the two-hour boot/100-minute benchmark stop policy with the agreed full-run runtime limit. Prevent legacy Gold-producing DAGs from running accidentally.
- CPU type and root size differ from the historical CloudFormation template. Reconcile desired state before any stack update; do not attempt to shrink the root back to 30 GiB.
- No full backfill, new capacity-planner Gold or production scheduler deployment has been started. Proceed only after the user finalizes the processing topology.
