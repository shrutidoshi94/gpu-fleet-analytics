# AWS execution evidence

These files preserve the project execution record after AWS teardown. They are not synthetic dashboard fixtures. Snapshot: November20,2026,12:36PM Pacific; all378 Silver transformations finished, final audits still running. Do not read this checkpoint as final pipeline success.

| Claim | Evidence | What to inspect |
|---|---|---|
| All four archives ingested into S3 Bronze | [AWS exports](aws/2026-11-20/) `bronze-*-success.json` |9,049 total files, source CRC/destination metadata/inventory checks |
| Real full-width data processed by Spark | `silver-*-day109.json`, `silver-execution-global.json` in the same directory | `synthetic_source:false`, application ID, row counts, code/source hashes, output inventories |
|378 transformation tasks completed under Airflow | [run snapshot](aws/2026-11-20/silver-run-status.json) |379 successes includes one plan task; remaining task is final audits |
| EC2 was used in Ohio | [resource snapshot](aws/2026-11-20/ec2-resource-snapshot.json) | Instance ID, m6a.2xlarge type, availability zone, launch time and running state |
| Measured CPU/Spark tradeoffs | [benchmark report](AWS_BENCHMARK_RESULTS.md), [raw results](silver-benchmark-results.json), [validation](silver-benchmark-validation.json) |9 timed cases,4 sampled final join audits, six-vs-three-thread comparison |
| Source edge cases investigated and repaired | [whole-trace profile](../data/whole-bronze-preflight.json), [observations](aws/2026-11-20/source-quality-observations.json), [test transcript](recovery-tests.txt) | All9,049 footers, rare schema variation, source inconsistency,12 passing recovery tests |
| Dashboard will survive teardown | [Gold design](../design/GOLD_TRANSFORMATION_PLAN.md) | Planned offline bundle; final capacity Gold/dashboard not implemented yet |

## Gold deployment checkpoint

The user authorized Gold November20. [Queued marker](aws/2026-11-20/gold-queued.json) and [SSM verification](aws/2026-11-20/gold-deployment-verification.json) confirm deployment on the existing worker and the armed handoff. Gold remains inactive until Silver is verified complete; these are not Gold processing results. All14 regressions passed ([transcript](gold-regression-tests.txt)); a final focused run also verified resumable offline export ([transcript](gold-export-tests.txt)). The deployment probe caught and corrected a service-finalization ordering issue before arming the handoff.

## Verify without AWS

Run `python3 examples/verify_evidence.py` from the repository root. It checks the saved file hashes, Bronze file totals, sampled row reconciliation, real-source indicators, and correspondence between current Silver code and saved production manifest hashes. It also reports the checkpoint state. No credentials or third-party Python packages are needed.

The benchmark predates the recovery code: its hash and stopped-worker state are historical, not current. Do not compare its `full_backfill_started:false` flag to the later full-run snapshot as though they were simultaneous. Saved task totals include Airflow's planning task.

## Provenance and limits

AWS JSON exports were downloaded via AWS CLI using the project profile; [sources.json](aws/2026-11-20/sources.json) maps saved names to original S3 keys. EC2 output uses a selected read-only describe-instances query. Exports retain non-secret resource identifiers for traceability. Benchmark JSON and the local test transcript are operator-collected validation evidence, not AWS-signed attestations.

[checksums.json](checksums.json) detects accidental changes against this checked-in snapshot; it does not independently prove AWS origin. Recruiters can cross-check infrastructure, DAGs, Spark code and matching execution metadata without live account access. Final run/audit results, final shutdown snapshot, Gold lineage and offline comparison results must be added when those stages finish. Hosted GitHub Actions success is not claimed until a real run is available.

No credentials, signed URLs, Airflow passwords, database dumps, billing screenshots or bulk trace data belong here. Bucket names, application IDs and instance IDs are identifiers, not authentication secrets. Original source data remains subject to its upstream terms.
