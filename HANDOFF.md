# GPU fleet analytics — agent handoff

Updated November 18, 2026 (America/Los_Angeles). Read this file first when resuming. Never store credentials in this repo.

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
