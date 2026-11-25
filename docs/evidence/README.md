# AWS execution evidence

These files preserve the AWS execution record for later review, including after teardown. They are not synthetic dashboard fixtures. **November21 checkpoint:** Silver and Gold are verified complete. Gold finished at12:56AM Pacific:185 daily partitions,186 successful Airflow tasks, seven compact tables,47,379,950-byte export,8h52m runtime. EC2 is stopped. This is the cloud-completion checkpoint. Local download, validation and the offline dashboard were completed November23; public deployment remains pending.

| Claim | Evidence | What to inspect |
|---|---|---|
| All four archives ingested into S3 Bronze | [AWS exports](aws/2026-11-20/) `bronze-*-success.json` |9,049 total files, source CRC/destination metadata/inventory checks |
| Real full-width data processed by Spark | `silver-*-day109.json`, `silver-execution-global.json` in the same directory | `synthetic_source:false`, application ID, row counts, code/source hashes, output inventories |
| Silver transformations, final audits and publication verification completed | [run snapshot](aws/2026-11-20/silver-run-status.json) |380 successful tasks includes plan,378 partitions and one task containing192 audits; verified_complete:true |
| EC2 was used in Ohio | [resource snapshot](aws/2026-11-20/ec2-resource-snapshot.json) | Instance ID, m6a.2xlarge type, availability zone, launch time and running state |
| Measured CPU/Spark tradeoffs | [benchmark report](AWS_BENCHMARK_RESULTS.md), [raw results](silver-benchmark-results.json), [validation](silver-benchmark-validation.json) |9 timed cases,4 sampled final join audits, six-vs-three-thread comparison |
| Source edge cases investigated and repaired | [whole-trace profile](../data/whole-bronze-preflight.json), [observations](aws/2026-11-20/source-quality-observations.json), [test transcript](recovery-tests.txt) | All9,049 footers, rare schema variation, source inconsistency,12 passing recovery tests |
| Gold/export finished and EC2 stopped | [Final evidence](aws/2026-11-21/) | verified_complete,185 day partitions,186 successful tasks, export schemas/checksums, successful finalizer and stopped worker |
| Dashboard will survive teardown | [Gold design](../design/GOLD_TRANSFORMATION_PLAN.md) | Portable bundle downloaded and verified; offline dashboard implemented. Public deployment remains pending |

## Gold deployment checkpoint

The user authorized Gold November20. [Queued marker](aws/2026-11-20/gold-queued.json) and [SSM verification](aws/2026-11-20/gold-deployment-verification.json) confirm deployment on the existing worker and the armed handoff. The queued/deployment files are earlier checkpoints. The [live-run snapshot](aws/2026-11-20/gold-run-status.json) records successful source pinning and the running pilot after verified Silver completion; this is an earlier checkpoint. The final evidence below supersedes its running state. All14 regressions passed ([transcript](gold-regression-tests.txt)); a final focused run also verified resumable offline export ([transcript](gold-export-tests.txt)). The deployment probe caught and corrected a service-finalization ordering issue before arming the handoff.

## Completion-checker recovery

Airflow finished all Silver transformations/audits at2:19PM. Its separate final checker failed to import silver.py, so the worker stopped and Gold did not start. The [failure checkpoint](aws/2026-11-20/silver-completion-import-failure.json) and [traceback](aws/2026-11-20/silver-checker-import-error.txt) preserve that event. Explicit project-path initialization fixed the entry point; an [isolated-interpreter regression](supervisor-import-test.txt) passed. Only final source/output/audit verification was rerun; no Spark transformations repeated. Silver verified at4:04PM and handed off to Gold. Original run timing is retained across the checker restart. The stopped interval was not EC2 compute time.

## Final Gold completion

[Run result](aws/2026-11-21/gold-run-status.json), [finalizer result](aws/2026-11-21/gold-service-result.json), [export manifest](aws/2026-11-21/gold-export-manifest.json), and [stopped EC2 snapshot](aws/2026-11-21/ec2-stopped-snapshot.json) record cloud completion. The export manifest contains table row counts, columns, file sizes and expected SHA256 hashes. These are metadata exports, not the47.4-MB data bundle. These cloud files remain metadata-only evidence. The local validation checkpoint below records the later download and checks.

## Local dashboard checkpoint — November23

[Local Gold verification](local-gold-verification.json) records9 expected file hashes, seven schemas/row counts,185 inventory checks against pinned reports and five workload populations matching6,570,124,684 observations. This checks snapshot integrity and consolidation, not a fresh raw-trace reprocessing. The dataset itself is gitignored.

[Dashboard tests](dashboard-tests.txt) block network sockets and cover the three-step story, cohort continuity, metric math, empty flagged windows, scenario controls, supporting scopes and tamper detection. [Story smoke](dashboard-story-smoke.txt) verifies the revised flow on the real bundle with network blocked. The earlier [real-data smoke](dashboard-real-data-smoke.txt) is retained as a historical checkpoint. [Pilot compatibility](pilot-dashboard-compatibility.txt) preserves the original Spark/JSON demo. The [runbook](../runbooks/DASHBOARD.md) specifies local setup and deployment boundaries. No public dashboard deployment or cloud teardown was performed.

## Verify without AWS

Run `python3 examples/verify_evidence.py` from the repository root. It checks the saved file hashes, Bronze file totals, sampled row reconciliation, real-source indicators, and correspondence between current Silver code and saved production manifest hashes. It also reports the checkpoint state. No credentials or third-party Python packages are needed.

The benchmark predates the recovery code: its hash and stopped-worker state are historical, not current. Do not compare its `full_backfill_started:false` flag to the later full-run snapshot as though they were simultaneous. Saved task totals include Airflow's planning task.

## Provenance and limits

AWS JSON exports were downloaded via AWS CLI using the project profile; [sources.json](aws/2026-11-20/sources.json) maps saved names to original S3 keys. EC2 output uses a selected read-only describe-instances query. Exports retain non-secret resource identifiers for traceability. Benchmark JSON and the local test transcript are operator-collected validation evidence, not AWS-signed attestations.

[checksums.json](checksums.json) detects accidental changes against this checked-in snapshot; it does not independently prove AWS origin. Recruiters can cross-check infrastructure, DAGs, Spark code and matching execution metadata without live account access. Silver completion evidence is saved. Gold completion/export metadata and the stopped-worker snapshot are saved. Full lineage is saved with the downloaded local bundle; local integrity/consolidation verification and offline dashboard smoke results are now saved. Public deployment and an independent backup remain pending. Hosted GitHub Actions success is not claimed until a real run is available.

No credentials, signed URLs, Airflow passwords, database dumps, billing screenshots or bulk trace data belong here. Bucket names, application IDs and instance IDs are identifiers, not authentication secrets. Original source data remains subject to its upstream terms.
