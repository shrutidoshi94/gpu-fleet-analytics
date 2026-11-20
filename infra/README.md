# AWS infrastructure

- `network-demo.json`: original CloudFormation worker, private S3 bucket, IAM role and security group.
- `data-volume.json`: historical temporary500-GiB ingestion volume; deleted after verified Bronze migration.
- `scripts/`: download/verification, bounded benchmarks and full Silver recovery/monitoring commands. Review placeholders and run IDs before reuse.

Actual processing used one m6a.2xlarge with60-GiB gp3, SSM management and no inbound application/SSH ports. CPU/root resizing was performed after the original deployment; the template retains historical defaults. See [evidence](../docs/evidence/README.md) and [HANDOFF](../HANDOFF.md). No AWS secret is required to read this repository.
