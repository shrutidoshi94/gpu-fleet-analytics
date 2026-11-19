# GPU fleet analytics — agent handoff

Updated November 19, 2026 (America/Los_Angeles). Read this file first when resuming.

CURRENT STATE: PROCESSING PAUSED at user request. All four S3 Bronze datasets and all S3-only pilots are verified. The temporary 500 GiB volume was explicitly approved for deletion and is now DELETED. EC2 is confirmed STOPPED on November 19, 2026, while the user reviews strategy tomorrow. Preserve the pause; do not restart or process without user instruction. Agree on PROCESSING_STRATEGY.md first.

## Goal and user decisions

Portfolio project: Alibaba GPU cluster trace v2026. Airflow orchestrates one relative trace day; Spark reads staged files on disk and publishes Bronze, Silver, Gold to S3. Streamlit reads a saved Gold JSON without AWS. Rerunning a day replaces its output. Budget: $150 in AWS credits; balance/applicability not verified. Start with the 204 MB network archive, day 109. Defer the 352 GB pod archive and GPU utilization analysis.

The user approved provisioning the small Ohio stack and running network day 109, checking a rerun, saving Gold locally, and stopping the worker afterward. Do not leave the worker running. Do not delete the S3 results without preserving and verifying local Gold.

## Repository and local runtime

- Checkout: `/Users/shrutidoshi/Documents/repo/gpu-fleet-analytics` (formerly `Data-Eng`).
- GitHub: https://github.com/shrutidoshi94/gpu-fleet-analytics
- Branch: `gpu-fleet-analytics`. The user requested a project commit after successful verification. Preserve any subsequent working-tree changes. No push has been performed.
- Original patch: `/Users/shrutidoshi/Documents/Codex/2026-11-18/new-chat/gpu-fleet.patch`. It was absent from the clean main checkout, then applied on the new branch. Do not apply it again.
- Local virtualenv: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/work/gpu-venv`.
- Local Java: `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.
- Shell originally points `SPARK_HOME` at Spark 4.2 and Java 21. Unset `SPARK_HOME`, use Java 17, and set `SPARK_LOCAL_IP=127.0.0.1` for this project.

## Completed steps

1. Applied the patch while preserving repository state; renamed GitHub repo and updated origin. Local directory also now uses the new name.
2. Ran the synthetic sample end to end: 48 observations, 24 Gold rows. Tests verify exact means, repeat-run replacement, neighboring-day preservation, incomplete-day rejection, and Streamlit with only saved Gold.
3. Verified Airflow 3.0.6 with `airflow dags test gpu_trace_day 2026-01-01` locally. Pinned Streamlit 1.48.1 for compatibility with Airflow's protobuf constraint; declared PyArrow explicitly. `pip check` passes. CI includes sample tests and Airflow validation, but GitHub Actions has not run.
4. Configured user-owned AWS CLI profile `gpu-fleet`. It authenticates via console login; refresh with `aws login --profile gpu-fleet` when needed.
5. Prepared `infra/network-demo.json`, validated it through AWS, and deployed stack `gpu-fleet-network-demo` after explicit approval.
6. Confirmed SSM online, no inbound security-group ports, and active two-hour stop timer. Installed Java 17, Python 3.11, Spark 3.5.5, Airflow 3.0.6, and requirements on the worker. `pip check` passes there too.
7. Downloaded only the network archive. Size 203904100 bytes; SHA256 `96bc687205a02f3f7e3fd3481aa0aadbe93b3f39790adee3b4010b6686d2ee53`. ZIP integrity passed. Day 109 contains 24 Parquet members totaling 29010782 uncompressed bytes.
8. First real Airflow run SUCCEEDED: 766207 Bronze observations, 766207 Silver rows, 24 Gold rows. Independent PyArrow validation checked hourly counts and means against Bronze. Every hour's server IDs were unique. Bronze has 24 Parquet files; Silver has 2.
9. The second real Airflow run SUCCEEDED. Counts stayed at 766207 Bronze/Silver rows and 24 Gold rows. Gold was byte-identical (SHA256 `a7b78aec7e7f2f0439b77b5771f6593cd5a2543b92b36ca464b509890ba7c622`). Stale files were removed from all three day-109 prefixes; the neighboring day-110 sentinel was preserved, then cleaned up.
10. Downloaded real Gold and run evidence to the laptop. Streamlit AppTest passed with only real Gold, no AWS environment variables or source archive. The local repo `output/gold/network/day=109/summary.json` now holds the real result; the synthetic backup remains separately named.
11. EC2 state confirmed `stopped` after saving results (November 18, approximately 5:43 PM Pacific). S3 and the 30 GiB EBS disk remain. No compute charges accrue while stopped; storage charges continue.

## AWS inventory

- Account: `498768607823`; caller role: `AccountFullAccessRole`.
- Profile: `gpu-fleet`; region: `us-east-2` (Ohio).
- Stack: `gpu-fleet-network-demo` (CREATE_COMPLETE).
- Worker: `i-09dc0bfe45b0d275f`, Name `gpu-fleet-network-demo`, t3.large, 2 vCPU / 8 GiB, standard CPU credits, encrypted 30 GiB gp3.
- Bucket: `gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6`.
- Existing VPC: `vpc-0aab5a7653f3d6cc3`; subnet: `subnet-01b6cfadcfa9d270f`.
- Worker stop timer: `gpu-fleet-stop.timer`; first deadline November 18 at 7:34 PM Pacific. Timer runs two hours after each boot. Verify its state after any restart.
- Instance role has SSM access and access only to the project bucket. No SSH key needed.
- Costs before credits: EC2 $0.0832/hour + public IPv4 $0.005/hour. Disk $2.40/month while retained. Stopping the instance does not remove disk/S3 storage charges.

## Locations and commands

Worker project: `/opt/gpu-fleet`; runtime settings: `/opt/gpu-fleet/env.sh`; venv `.venv`; archives `archives/`; logs `logs/`. Run as `ec2-user`, source env.sh first. Spark uses local[2], 2 GiB driver memory, four shuffle partitions. Airflow uses SQLite for this demonstration and `dags test` executes the DAG; a persistent scheduler/UI has not been started.

S3 prefixes: `bronze/network/day=109/`, `silver/network/day=109/`, `gold/network/day=109/summary.json`, `source/`, `run-logs/`. Publication across layers is sequential, not atomic; consume Gold only after task success.

Local orchestration scratch directory:
`/Users/shrutidoshi/Documents/Codex/2026-11-18/are/work/aws-run/`

It contains `manage.py`, `state.json`, `bootstrap.sh`, `first.sh`, `rerun.sh`, `verify.py`, source.tar.gz, command IDs and fetched SSM results. `manage.py` calls AWS using the explicit profile/region; it does not store credentials.

From `/Users/shrutidoshi/Documents/Codex/2026-11-18/are`:

```bash
python3 work/aws-run/manage.py poll rerun
python3 work/aws-run/manage.py state
```

Gold backup:

```bash
aws s3 cp s3://gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6/gold/network/day=109/summary.json \
  /Users/shrutidoshi/Documents/Codex/2026-11-18/are/outputs/alibaba-network-day-109.json \
  --profile gpu-fleet --region us-east-2
```

Stop worker after verification:

```bash
aws ec2 stop-instances --instance-ids i-09dc0bfe45b0d275f --profile gpu-fleet --region us-east-2
aws ec2 describe-instances --instance-ids i-09dc0bfe45b0d275f --profile gpu-fleet --region us-east-2 \
  --query 'Reservations[0].Instances[0].State.Name' --output text
```

## Remaining work at this checkpoint

- No active run remains. EC2 is confirmed stopped; pipeline, rerun, backup, and offline dashboard checks are complete.
- Gold: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/outputs/alibaba-network-day-109.json`.
- Evidence: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/outputs/aws-run-verification.json`. Full logs: `work/aws-run/saved-logs/` under that chat workspace.
- Run local dashboard using the existing venv: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/work/gpu-venv/bin/streamlit run dashboard.py` from the repo root.
- Later: optionally publish the repo/CI, improve dashboard, and plan additional days. The 352 GB workload phase is not authorized as part of this run.
- Full teardown is separate: deleting the CloudFormation stack terminates EC2 and deletes its disk, but intentionally RETAINS the bucket. After verifying Gold backups, explicitly empty/delete the bucket if requested. Never delete unrelated account resources.

## Scope clarification after the first AWS experiment

The complete 204 MB network ZIP was downloaded on EC2, but only day 109 was extracted and processed. Two successful executions cover that same day: initial run and replacement verification. Network days 110–115 have not been processed. Airflow executed the medallion pipeline via `dags test`; a continuously running scheduler has not been configured. Extracting the entire archive is optional: the current day-at-a-time extraction reduces disk use and satisfies the daily processing design. The full 352 GB pod archive has not been downloaded and will require a separate capacity/cost decision before GPU utilization analysis.

## 500 GiB data volume added (November 18, 2026)

User approved the proposed separate 500 GiB gp3 disk. Stack `gpu-fleet-data` is CREATE_COMPLETE; template `infra/data-volume.json`. Volume `vol-028dceeb16c897cfc` is encrypted, in us-east-2a, attached to worker `i-09dc0bfe45b0d275f` as `/dev/sdf` (Linux detects `/dev/nvme1n1`). XFS filesystem mounted at `/data/gpu-fleet`, persistent via UUID in `/etc/fstab` with `nofail`. Verified size 500 GiB, approximately 497 GiB available, and ec2-user write access.

Worker `/opt/gpu-fleet/env.sh` now sets:
- `TRACE_DIR=/data/gpu-fleet/archives`
- `SPARK_LOCAL_DIRS=/data/gpu-fleet/scratch`
- `TMPDIR=/data/gpu-fleet/scratch`

The existing network ZIP was copied to the new archive directory. The original env file is backed up as `/opt/gpu-fleet/env.before-data-volume.sh`. `infra/scripts/mount-data.sh` records the executed setup and checks the exact volume serial before formatting a blank disk. Its volume serial is specific to this deployment; do not use it unchanged on another disk.

The initial attachment attempt raced worker startup and rolled back cleanly, deleting its attempted volume. Retrying after the worker reached running succeeded. Stop timer was checked active after setup. A worker stop was requested after verification.

No additional archives were downloaded and no new trace days were processed during this storage change. Next: download remaining archives with free-space checks, inspect ZIP uncompressed sizes per day before extracting, and extend pipeline support for server inventory and execution summary. A 500 GiB volume does not guarantee room for all archives fully extracted simultaneously.

Cost: the additional data volume is about $40/month ($1.33/day) while retained, even if EC2 is stopped. The original 30 GiB disk and S3 charges are additional. The separate data stack owns the disk: delete `gpu-fleet-data` before deleting the worker stack, only after saving required data. Instance termination alone must not be treated as data-volume cleanup.

Post-expansion EC2 state confirmed `stopped`. Storage setup completed successfully; no active processing remains. Storage expansion files are uncommitted after project commit `260eab9`.

## Remaining archives download started (November 18, approximately 5:58 PM Pacific)

User explicitly approved downloading the remaining three archives. EC2 was started; `/data/gpu-fleet` mount and free space verified. Downloader is running as ec2-user in systemd transient service `gpu-fleet-download`. Scripts are saved in `infra/scripts/download-archives.sh` and `download-progress.sh`. An initial log-directory ownership issue was corrected before downloads started.

Order: server_hourly (3080121387 bytes), job_execution_summary (1188295031 bytes), pod_hourly (351803513445 bytes). Already downloaded network archive stays intact. Downloads use curl resume/retries, enforce a 20 GiB free-space reserve, compare exact expected size, record local SHA256, and inspect ZIP directory / per-day uncompressed sizes. This is not full CRC validation or a trusted upstream-checksum comparison. No extraction or pipeline transformation is started by the downloader.

Live status: `/data/gpu-fleet/download-logs/status.json`; curl logs and per-archive JSON evidence are in the same directory. Current partial files are in `/data/gpu-fleet/archives`. Worker stop timer remains at two hours per boot (current deadline approximately 7:56 PM Pacific); downloader has a 110-minute runtime limit. If interrupted, partial downloads persist on the data disk and can resume. A service status of success means all three passed the documented checks; a successful SSM launch alone does not mean downloads completed.

From the chat workspace use `python3 work/aws-run/manage.py send work/aws-run/download-progress.sh`, then `python3 work/aws-run/manage.py poll download-progress`. To resume after a stop, start EC2, verify mount/SSM, clear a failed transient service if necessary, and rerun the download script. Keep checking actual download state; do not mistake stale JSON `running` after a forced stop for active work. Stop EC2 after completion or a terminal failure. Additional EBS costs continue while stopped.

### Download runtime guard update (approximately 6:01 PM Pacific)

The measured download rate requires a longer session for the 352 GB archive. Worker remains RUNNING and downloading. For this boot only, the two-hour timer was stopped after a 12-hour fallback shutdown was installed (November 19 at 6:00 AM Pacific). Systemd downloader RuntimeMaxUSec is verified `11h`; its root ExecStopPost runs shutdown on completion, failure, or timeout. These settings were verified after daemon reload. `systemctl set-property RuntimeMaxSec` was unsupported; the runtime drop-in successfully supplies the setting. See `infra/scripts/download-runtime.sh` (corrected replay script) and `download-controls.sh`. This transient override disappears on reboot, and the normal two-hour timer remains enabled for future boots. Compute plus public IPv4 cost is approximately $1.06 for 12 hours, plus disk/S3 costs. No scheduler or transformations are running. Completion must be checked from status/evidence; downloads are NOT yet complete.

## Active milestone monitoring

User requested notification if downloads stop and at 25%, 50%, 75%, and 100%, plus an ETA. Progress is measured by actual bytes across all four archives (356275833963 total bytes), including the already complete network archive. Server inventory and execution summary downloads are now complete and checked. Workload download is active.

Worker `gpu-fleet-download-reporter` publishes `/data/gpu-fleet/download-logs/progress.json` once per minute to `s3://gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6/run-logs/downloads/progress.json`. Downloader ExecStopPost publishes final status/evidence before shutdown. Script: `infra/scripts/install-download-monitor.sh`. Completion/failure metadata, excluding curl logs, is synced to the same S3 prefix.

This chat is monitoring in-session, not via an automation. Local monitor: `/Users/shrutidoshi/Documents/Codex/2026-11-18/are/work/aws-run/watch-download.py`; process session 9239. It checks telemetry every 50 seconds, records `progress-history.jsonl`, calculates ETA, and emits milestone, complete, failure, stopped-worker, stale-telemetry, or stalled-transfer events. Do not claim milestones happened without observed evidence. If this chat stops or the local process exits, another agent must resume monitoring explicitly. Authentication/network failures in monitoring do not prove the remote download stopped.

### Download milestone observed

25% milestone reached at 7:54 PM Pacific on November 18: telemetry reported 25.1239%, 89510220294 / 356275833963 bytes total; workload archive 85037899776 bytes. Service active, no error; server inventory and execution summary verified complete. User notified. Estimated finish remains about 1:45 AM Pacific November 19 at approximately 12.7 MB/s. Remaining notifications: 50%, 75%, 100%, or any stopped/stalled download. Monitoring process session 9239 remains active.

Credit caveat: AWS Free Tier plan API returned no plan data for account 498768607823. Promotional credit applicability/remaining balance is unverified; user was directed to Billing → Credits → Applicable Products. t3.large is not marked Free Tier eligible; promotional credit coverage is a separate question. Do not represent estimated subtraction from $150 as a verified balance.

50% milestone observed at 9:52 PM Pacific November 18: 50.0672%, 178377380358 / 356275833963 bytes total; workload 173905059840 bytes. Service active, no error, free disk 352266006528 bytes. User notified; expected finish approximately 1:50 AM November 19. Remaining notifications: 75%, 100%, or stop/stall.

75% milestone observed at 11:50 PM Pacific November 18: 75.0186%, 267273306630 / 356275833963 bytes total; workload 262800986112 bytes. Service active, no error, free disk 257777119232 bytes. User notified; approximately two hours remaining. Remaining notification: 100% / verified completion, or stop/stall. Local monitor session 9239 remains active.

## User changed shutdown / processing plan near download completion

User explicitly requested keeping the worker running after download, then transferring SSD contents into S3 Bronze and processing Silver/Gold with Airflow/Spark. Scheduled shutdown was canceled; normal stop timer is inactive for this boot; downloader ExecStopPost now only publishes status (no shutdown). Verified via SSM: active downloader, 11h runtime cap, no scheduled shutdown. See `infra/scripts/keep-worker-running.sh`. Worker must NOT be stopped automatically at download completion.

EBS cannot be paused; detaching does not stop storage charges. A data-volume deletion may happen only after Bronze ingestion is verified and the pipeline is proven to read S3 without this disk. Current pipeline reads local staging, so code changes and tests are required before deletion. Current env scratch paths point at data disk and must be moved back to root-volume scratch before detachment. All four dataset ingestion/processing is now requested; full workload CRC validation is not yet complete. Monitor process session 9239 and functions cell 180 remain active, awaiting 100% and verification. Latest observed progress was 97.08%.

## S3-first implementation in progress

Added `ingest_bronze.py`: streams Parquet ZIP members to dataset/day/hour S3 Bronze keys, validates source CRC while reading, verifies destination sizes/metadata, checks full key/size inventory, and publishes `manifests/bronze/{dataset}.success.json` only after success. Resumes uploads by matching per-object source CRC and size. It removes obsolete keys only within the selected dataset's Bronze prefix. Worker installed boto3 against Airflow constraints. Small archive ingestion (network/server/execution) is running via SSM command `001363e8-9997-4d86-a8c9-a136a5e3f748`; local command file `work/aws-run/ingest-small-command.json`; logs `/opt/gpu-fleet/logs/bronze-*.log`. Do not delete the data volume until all four datasets have success manifests and S3-based processing has passed.

Modified `pipeline.py` with `--from-bronze`, requiring dataset success marker then reading s3a:// Bronze directly through hadoop-aws 3.3.4 (matches Spark 3.5.5 Hadoop). Silver/Gold still stage locally before S3 sync replacement. Added server inventory and execution duration transformations. `dags/gpu_trace.py` accepts from_bronze; added serial mapped-task `gpu_medallion_backfill` (start_day/end_day default 109 pilot; 0..184 for full run). Dashboard supports all four reports. These changes are not yet validated on EC2 or committed. Local regression run in session 45540; log in chat workspace `work/s3-upgrade-tests.log`. Need test S3 read, metrics, all four pilot outputs, and retry semantics before launching full backfill.

### 100% download / S3 migration checkpoint

All four compressed files reached their exact expected sizes at approximately 1:47 AM Pacific November 19 (356275833963 bytes total); user notified of 100% transfer. Workload full-file SHA256 verification is still running; no full ZIP CRC sweep has been performed by the downloader. Source member CRC validation happens during Bronze ingestion.

Network/server/execution Bronze ingestion succeeded: respectively 168 files / 203870478 bytes, 4440 files / 3079229209 bytes, and 1 file / 1188294827 bytes. Success manifests are in S3 `manifests/bronze/`. Workload Bronze ingestion is active under systemd `gpu-fleet-bronze-pod`, runtime max 8h; `/opt/gpu-fleet/logs/bronze-pod.log` shows progress every 100 files (4440 total). Launch SSM command `b026a4d0-df90-4f49-92cd-43d0f741552c`. This service does not stop EC2.

Spark S3A network day-109 pilot succeeded with 24 Gold rows / 766207 observations using ONLY S3 Bronze and root-volume scratch. Its surrounding diagnostic shell returned exit 120 solely because `pip show | head` broke stdout after the successful pipeline; logs explicitly confirm the validation passed. Server day-109 Airflow pilot also succeeded. Execution pilot is currently running via SSM `a8516da1-980b-404c-a672-af3843fdde92`. Boto3 constrained version is 1.39.11; requirements now pin that version. Local regression suite passed all three tests after S3 pipeline/dashboard/DAG changes.

Worker env now uses root-volume `/opt/gpu-fleet/scratch` for TMPDIR and SPARK_LOCAL_DIRS. TRACE_DIR still points to data-disk archives for ingestion. S3 pipeline does not read it with --from-bronze. Root disk had about 26 GiB free at checkpoint. Independent Gold validator is in `examples/verify_gold.py` and S3 `source/verify_gold.py`; run it for all four pilot outputs before removing the data volume. Workload pilot must wait for its Bronze success manifest. `work/aws-run/processing-progress.sh` and `diagnose-processing.sh` provide progress/log probes. No data disk has been detached/deleted. No full backfill has started. Worker remains running per user request; no scheduled shutdown or downloader shutdown hook remains.


### Current processing checkpoint (approximately 2:10 AM Pacific November 19)

Server and execution Airflow S3 pilots succeeded. Gold counts: network 766207 observations / 24 groups; server day109 775104 server-hours / 12 GPU-spec groups; execution 40522321 records / six workload groups. Independent PyArrow checks are active via SSM `70146954-815d-4563-ac48-889ee09c73fd`, command label `validate-small-gold`. It compares group counts/means against S3 Bronze and metadata row counts against actual S3 Silver.

Workload Bronze: at least 700/4440 files copied (~16% by count, not bytes). User asked whether the large EBS disk is still needed; explained it must remain until upload and S3-only workload pilot are verified, then deletion is needed to stop its storage fees. No deletion approval has been requested yet, and no disk removal has occurred.

The ingester was optimized with a forward-only ZIP-member reader to avoid Boto3 seeking through each ZIP member twice. Local tests passed. Active workload ingestion was safely stopped and resumed with already-verified objects skipped (SSM `5c56709c-2946-4e5b-b2be-d4c4d9b3aeb1`). Systemd runtime remains 8h and there is no shutdown hook for this service. Some aborted multipart uploads from interruption may remain until the bucket's one-day abort lifecycle clears them. Final manifest must still pass key/size inventory.

Next steps: collect independent small Gold checks and archive SHA completion; wait for workload Bronze success manifest; execute workload day109 and mapped Airflow pilot from S3; independently validate workload Silver/Gold and rerun replacement; ask for explicit disk deletion approval only after that evidence is concrete; unmount and delete data stack if approved; execute full 0..184 serial backfill (378 dataset/day tasks) and save all Gold locally. Keep worker running to continue processing as requested. No full backfill has started yet.

### Independent small-dataset checks complete

`validate-small-gold` SSM completed successfully: network day109 766207 rows / 24 groups; server day109 775104 rows / 12 groups; global execution 40522321 rows / six groups. PyArrow independently grouped source Bronze, checked means/counts, and checked actual S3 Silver Parquet metadata row counts. Downloaded Gold backups to chat outputs/gold and copied them into repo output/gold for the local dashboard. AppTest rendered all three REAL saved reports without exceptions. Workload remains pending. Full source/verification evidence is under chat `work/aws-run/gold-validation/` and bucket `run-logs/gold-validation/`.

A second local monitor session 14702 (`work/aws-run/watch-bronze.py`) tracks workload upload file-count milestones and service failure through telemetry; record file `bronze-progress-history.jsonl`. Worker reporter was extended to publish `bronze_pod` status into progress.json. Session 9239 / functions cell199 still watches archive SHA/metadata verification completion. No automation was created. If this chat ends, another agent must actively resume monitoring; remote services continue running.

## README refreshed at user request

README.md was rewritten to consolidate the four-dataset scope, accurate active migration status, S3-first architecture, verified pilots, local setup, ingestion validation, Airflow dataset selection/backfill commands, offline Gold retention, costs/credit caveat, and safe EBS teardown. Stale claims that the worker is stopped or workload phase deferred were removed. Code fences/local links and git diff whitespace checks passed. Documentation and implementation changes remain uncommitted.

Current cost discussion: user asked about more CPUs / EC2 workers. Ohio Pricing API verified m6i.xlarge (4 vCPU /16 GiB) $0.192/h and m6i.2xlarge (8 vCPU /32 GiB) $0.384/h. Recommended considering a single 8-CPU worker after timing workload day109. No resize/extra worker was authorized or performed. Full 378-task runtime estimate on existing t3.large is provisional (20–50h, could be longer with CPU-credit throttling); benchmark is pending.

Workload-only pilot script is staged in chat `work/aws-run/pilot-pod.sh`: runs mapped Airflow DAG for day109/datasets=[pod], asserts DAG success, records elapsed time to S3, then independently validates Bronze/Silver/Gold. Its TRACE_DIR is deliberately nonexistent to prove no ZIP dependency. Do not launch until pod Bronze success manifest exists. Source DAG with dataset-selection option is staged in S3 source/gpu_backfill.py. Local monitors: session9239 (archive verification, functions cell199) and session14702 (Bronze ingestion, functions cell243); next outcome expected is complete Bronze inventory then workload pilot.


## All Bronze datasets verified (approximately 2:52 AM Pacific November 19)

Workload Bronze ingestion completed successfully: 4440 Parquet objects, 351802613287 bytes; optimized resume uploaded 3901 files and reused 539 verified files. Source CRC, destination sizes/metadata, and full key/size inventory checks passed. All four success manifests were downloaded to chat `work/aws-run/bronze-manifests/`; compact evidence is `outputs/bronze-migration-verification.json`. Total inventories: network168 files/203870478 bytes, server4440/3079229209, execution1/1188294827, pod4440/351802613287.

Started workload-only mapped Airflow pilot via SSM `df740382-5a68-45d5-bd13-cd04ceaef897` (label `pilot-pod`). Input is S3 Bronze; TRACE_DIR points to nonexistent archives to prove no local ZIP dependence; TMPDIR/SPARK_LOCAL_DIRS use root disk. The job records monotonic Airflow runtime to S3 `run-logs/workload-pilot/timing.json`, then runs independent PyArrow Bronze/Silver/Gold validation. Poll `python3 work/aws-run/manage.py poll pilot-pod` from chat workspace. The local Bronze monitor session14702 has exited successfully; download/hash monitor session9239/functions199 remains active.

No SSD deletion has occurred. Wait for archive readers to finish and workload pilot/checks to pass, then request explicit data-volume deletion approval. User has asked about 8-CPU processing but has not selected/authorized a resize; use pilot timing to prepare that concrete decision before a full backfill. Worker remains running, per explicit instruction.


## Explicit pause, disk deletion, and strategy discussion

User requested pause, then explicitly requested deleting volume vol-028dceeb16c897cfc, then asked to agree on a processing strategy before starting further processing. Workload rerun SSM command b7a969f7-5d90-4b26-b059-5563524624fe was canceled (status Cancelled, code137). No full backfill or resize has started. Day109 workload may need restoration via a successful rerun; test probe files may remain in Silver/Gold and neighboring day110. All four first pilots and independent validations had passed before cancellation.

Disk deletion completed: stopped reporter, verified archive reader/ingestion services inactive, preserved JSON archive evidence to root logs and S3, checked exact volume serial and 500 GiB size, updated TRACE_DIR to system-disk archives, removed only the data UUID from fstab, unmounted, and deleted the gpu-fleet-data CloudFormation stack. Stack deletion waiter passed. AWS describe-volumes now returns InvalidVolume.NotFound for vol-028dceeb16c897cfc. The original worker/root volume and S3 are retained. No processing was started for deletion. Worker remains running per user's earlier instruction.

All archive verification is complete, including workload SHA256 a36da575f041edde553425c668d0a507e2fef0c665148fe7a562b884b1a536af. Source evidence is S3 manifests/source-archives.json and chat outputs/archive-download-verification.json. All Bronze success manifests are verified. Workload pilot took 99.653 seconds including mapped Airflow orchestration and independently verified 2816548 observations / six groups; its offline Gold AppTest passed. Evidence: outputs/workload-pilot-verification.json. SourceGold backups are outputs/gold and repo output/gold.

Read PROCESSING_STRATEGY.md for the proposed plan. It is a discussion draft, not permission to execute. Prior resource-choice prompt's CPU question was unanswered; do not interpret a later preference as approval to resume processing while the explicit strategy pause stands. Historical monitor sessions9239 and14702 have finished. Monitor reporter is stopped. Need no active download/ingestion monitoring now.


## User requested EC2 stop for strategy review

November 19, 2026: user requested stopping EC2 for now and reviewing the processing strategy tomorrow. Stop requested for i-09dc0bfe45b0d275f. Preserve all S3 data and the 30 GiB root disk. The 500 GiB data volume is already deleted. Do not restart EC2 or resume processing automatically. No reminder/automation was requested or created. Confirm the stopped state and record it below.

AWS waiter and describe-instances confirmed `stopped`. No processing or restart is scheduled. S3 and the 30 GiB system volume remain; their storage charges continue.


## Actual Bronze row review and current-state commit

User requested ten rows per table in PROCESSING_STRATEGY.md to understand insight potential, then requested committing the current state. Read-only S3 sampling was performed locally with PyArrow; EC2 stayed stopped. Daily tables use day109/hour00; execution uses the global file. First ten physical rows were captured with full columns/types/provenance. Because the first ten workload rows all request zero GPUs, ten additional positive-GPU examples from the first 4096 rows of the same source object were added. All are examples, not representative samples or ranked top values. Raw CSV/JSON and alias mapping are in docs/bronze_samples. No transformation code or AWS processing was run for this review.

Strategy now includes observed nullable fields, fractional requests, missing vs zero SM, negative delay sentinel candidates (including ready=true cases), CPU/request utilization above1, safe joins, interpretation limits, and twelve potential Gold analyses. Main proposal is to preserve more workload dimensions and CPU-only records in clean Silver, with a GPU-specific view, instead of the current six-column/positive-GPU-only prototype. This proposal is not implemented or authorized to run yet.

Read-only cost check: Ohio has only this stopped t3.large, one retained30 GiB gp3 volume, no NAT gateways; project bucket holds359114680983 bytes (~334.45GiB). Verified S3 Standard rate $0.023/GiB-month gives ~$7.69/month plus root disk $2.40/month, approximately $0.34/day before credits/requests. Other-region inventory checks were denied by organization policy; do not claim an account-wide audit. No AWS resources changed during sampling/cost inspection.
