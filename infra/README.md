# AWS reproduction

The completed run used one m6a.2xlarge (8 vCPU, 32 GiB), a 60-GiB gp3 root, Spark local[6] with a 12-GiB heap, and Airflow 3.0.6 / LocalExecutor / PostgreSQL 15. No distributed Spark cluster was used.

## Current deployment path

`worker.json` creates a private S3 bucket, a CPU worker, bucket-scoped IAM access and SSM management with no inbound ports. Defaults match the measured machine. Compute type, root size and boot limit are parameters. `bootstrap-worker.sh` installs Python 3.11, Java 17, dependencies and PostgreSQL, creates a private environment file, migrates Airflow, and starts its API/scheduler/DAG processor. Only Silver and Gold DAGs are loaded.

These commands provision billable resources. They were not executed during the repository cleanup. Use a new stack name; the launcher refuses to update an existing stack.

```bash
export PROFILE=gpu-fleet AWS_DEFAULT_REGION=us-east-2
export VPC_ID=YOUR_VPC SUBNET_ID=YOUR_PUBLIC_SUBNET
export STACK_NAME=gpu-fleet-repro
bash infra/deploy.sh
```

The launcher prints the bucket, instance and exact Git SHA archived to S3. After the worker registers with SSM, use those values:

```bash
python -m scripts.worker_bootstrap_params --bucket YOUR_BUCKET --sha YOUR_SHA > /tmp/gpu-bootstrap.json
aws ssm send-command --profile gpu-fleet --region us-east-2 \
  --instance-ids YOUR_INSTANCE --document-name AWS-RunShellScript \
  --parameters file:///tmp/gpu-bootstrap.json --timeout-seconds 3600
```

Inspect the returned command with `aws ssm get-command-invocation`. Bootstrap refuses to overwrite `production.env.sh` and does not trigger a processing run. Inspect `systemctl status gpu-airflow-{api,scheduler,dag-processor}` and `gpu-fleet-stop.timer` before processing. The API binds loopback port 8080; use an SSM port-forwarding session for local access.

The configurable boot timer defaults to 48 hours; task timeouts are shorter. A timer is not a billing cap. Stop the worker when finished. Keep the bucket until results are exported and verified; CloudFormation intentionally retains it.

## Process data

The four compressed archives total about 356 GB and cannot fit on the 60-GiB processing root. For a full reproduction, create a separate temporary archive stack using `archive-volume.json`, with the worker instance ID and its availability zone:

```bash
aws cloudformation create-stack --profile gpu-fleet --region us-east-2 \
  --stack-name gpu-fleet-archives --template-body file://infra/archive-volume.json \
  --parameters ParameterKey=InstanceId,ParameterValue=YOUR_INSTANCE \
    ParameterKey=AvailabilityZone,ParameterValue=YOUR_WORKER_AZ
aws cloudformation wait stack-create-complete --profile gpu-fleet --region us-east-2 --stack-name gpu-fleet-archives
aws cloudformation describe-stacks --profile gpu-fleet --region us-east-2 --stack-name gpu-fleet-archives --query 'Stacks[0].Outputs'
```

This provisions a billable 500-GiB disk. On the worker, `sudo bash infra/mount-archives.sh YOUR_ARCHIVE_VOLUME_ID` identifies that exact NVMe volume before formatting a blank device. Supply only the new archive volume ID. The script refuses a partitioned or non-XFS existing device.

Run the download/ingestion job independently of the laptop:

```bash
sudo systemd-run --unit=gpu-fleet-ingest --uid=ec2-user \
  --property=WorkingDirectory=/opt/gpu-fleet --property=RuntimeMaxSec=47h \
  --property='ExecStopPost=+/usr/sbin/shutdown -h now' \
  /bin/bash /opt/gpu-fleet/infra/ingest-archives.sh /data/gpu-fleet/archives
```

The script resumes downloads, checks the recorded archive sizes/free space, records local SHA256 values, streams ZIP members to Bronze with CRC verification, and publishes a footer-derived source-observation catalog before Silver. Recorded size and a locally calculated hash are not a trusted upstream checksum. Check `journalctl -u gpu-fleet-ingest` and all four Bronze success markers. The job stops the worker when finished. Restart it explicitly before processing.

Delete the temporary archive stack only after confirming Bronze inventory/CRC evidence and deciding the local ZIP copies are no longer needed; deletion permanently removes those ZIPs. The 60-GiB processing root is separate. Existing original Bronze data can be reused instead of repeating ingestion only after verifying its manifests and access; a new bucket starts empty.

For the full dataset, start the monitored Silver stage on the worker:

```bash
sudo bash infra/run-stage.sh silver YOUR_NEW_SILVER_RUN_ID
```

This triggers all four datasets and all covered days, then runs the completion verifier. The systemd service saves progress, enforces a 31-hour limit and stops EC2 on exit. Check the resulting `run-logs/silver/status.json` for `verified_complete:true`; do not start Gold from a failed or partial result. The configured checkpoint key is shared by the verifier and Gold pinning.

Restart the same worker explicitly for Gold, wait for its services/DAGs to register, then run:

```bash
sudo bash infra/run-stage.sh gold YOUR_NEW_GOLD_RELEASE_ID
```

Gold runs under a 16-hour service limit and also stops EC2 on exit. These are two explicit stages, so compute is not left running between them. Local disconnection does not terminate the systemd job. Read progress with `journalctl -u gpu-fleet-STAGE-ID` and inspect S3 run reports. These launchers process the full trace; use the [Silver](../docs/runbooks/SILVER_RUNBOOK.md) and [Gold](../docs/runbooks/GOLD_RUNBOOK.md) runbooks for individual partitions and local fixtures. A newly created bucket does not contain the original trace.

Preserve orchestration metadata before teardown: `pg_dump` the Airflow database and keep the dump privately. Public evidence should contain selected run reports and manifests, never credentials or database dumps.

## Validation boundary

The new template is checked with cfn-lint and the scripts with shell syntax checks. This revised bootstrap has not been run on a new AWS machine. The [recorded evidence](../docs/evidence/README.md) proves the original deployment, not this revised deployment path.

`history/` preserves original templates and one-off operations for the completed run. They use old paths, dated archives and historical sizing. They are not the current deployment instructions.
