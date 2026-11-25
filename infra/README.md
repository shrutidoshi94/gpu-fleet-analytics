# AWS infrastructure

- `network-demo.json`: original CloudFormation worker, private S3 bucket, IAM role and security group.
- `data-volume.json`: historical temporary500-GiB ingestion volume; deleted after verified Bronze migration.
- `scripts/`: download/verification, bounded benchmarks and full Silver recovery/monitoring commands. Review placeholders and run IDs before reuse.

Actual processing used one m6a.2xlarge with60-GiB gp3, SSM management and no inbound application/SSH ports. CPU/root resizing was performed after the original deployment; the template retains historical defaults. See [evidence](../docs/evidence/README.md). No AWS secret is required to read this repository.

## Recorded environment

Account `498768607823`, profile `gpu-fleet`, region `us-east-2`. Worker `i-09dc0bfe45b0d275f`; retained 60-GiB gp3 root `vol-0aa7b9d7b5285efac`. Bucket `gpu-fleet-network-demo-tracebucket-ojk2fpexu8a6`. The temporary 500-GiB volume was deleted after verified Bronze migration. These identifiers describe the recorded project environment; check AWS for live state.

Airflow 3.0.6 used LocalExecutor with PostgreSQL 15; Spark used local[6] with a 12-GiB driver heap. Production loaded only the Silver and Gold DAGs. Private worker configuration was sourced from `/opt/gpu-fleet/production.env.sh`; never commit that file or database dumps.

The last saved completion snapshot records stopped EC2. The offline dashboard needs no running worker. Retain S3 data and the root volume until a verified independent backup exists and teardown is explicitly requested. Historical deployment scripts download archived source layouts; review packaging before reusing them with the reorganized checkout.
