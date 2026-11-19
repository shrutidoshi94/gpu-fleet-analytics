# GPU fleet analytics: first AWS run

Prepared November 18, 2026. Profile: `gpu-fleet`. Region: `us-east-2` (Ohio).

Authentication succeeded. Read-only checks found an existing default VPC with a working internet-gateway route and public subnets. No resources have been provisioned for this project.

## Proposed stack

CloudFormation stack name: `gpu-fleet-network-demo`.

| Resource | Configuration |
| --- | --- |
| Worker | One on-demand `t3.large`: 2 vCPU, 8 GiB RAM, Amazon Linux 2023 x86_64 |
| CPU credits | Standard mode; avoids surplus-credit fees, but can throttle sustained CPU work |
| Disk | 30 GiB encrypted gp3; deleted when the instance is terminated |
| S3 | One generated-name private bucket, encryption enabled, public access blocked, HTTPS required |
| Access | Dedicated instance role: SSM management plus access to this bucket only |
| Networking | Existing VPC/subnet, one automatically assigned public IPv4, no inbound security-group rules |
| Runtime limit | Systemd timer stops the worker two hours after each boot; verify timer after launch |

Use VPC `vpc-0aab5a7653f3d6cc3` and subnet `subnet-01b6cfadcfa9d270f` in `us-east-2a`. The current Amazon Linux public SSM parameter resolves to `ami-04f1cb199bc8debfd`; the template resolves that parameter at deployment time. The public address supports downloads and SSM without a NAT gateway. Manage the worker with SSM; no SSH key or inbound SSH port is required.

The template configures infrastructure and the stop timer. Project dependencies and the working-tree source will be installed after launch. It does not fetch an unpublished branch from GitHub.

## Cost estimate before credits

AWS Pricing API checked for Ohio on November 18, 2026:

- Linux `t3.large`: $0.0832/hour.
- Public IPv4: $0.005/hour, per the AWS VPC pricing page.
- gp3 storage: $0.08/GB-month, or $2.40/month for 30 GiB, prorated while the volume exists.
- S3 storage and requests are usage-based and additional.

Two running hours cost about $0.18 for EC2 plus IPv4. Disk is approximately $0.007 if created and deleted within those two hours. A single network-day experiment should be comfortably below $1 if promptly cleaned up, but this is an estimate, not a spending cap. Taxes and unrelated account usage are excluded. Credit balance and credit applicability have not been verified.

Stopping EC2 retains the disk and S3 data, which keep accruing storage charges. The timer is a runtime guard, not a billing cap, and must be checked after launch. No paid managed Airflow service, EMR cluster, NAT gateway, or GPU worker is included.

Pricing references: https://aws.amazon.com/vpc/pricing/ and https://aws.amazon.com/ebs/pricing/ . The regional EC2 and gp3 amounts were verified using the authenticated AWS Pricing API.

## Run and retain the result

1. Deploy the template after approval and wait for stack completion.
2. Verify SSM connectivity, the two-hour stop timer, and remaining local disk.
3. Transfer this repository's current working-tree source to the private bucket/worker; install Java 17, Python 3.11, and the tested dependencies.
4. Download only `asi_opensource_network_hourly.zip` (203,904,100 bytes). Do not download the workload archive.
5. Run network day 109 through Airflow with `TRACE_BUCKET` pointing at the stack bucket. Spark reads staged local files and publishes Bronze, Silver, then Gold.
6. Rerun the same day and compare row counts and Gold content; verify S3 replacement semantics.
7. Download Gold to the laptop, verify Streamlit works with that file alone, and retain a separate backup.
8. Stop the worker after the run. After verification, delete the stack to terminate the worker and delete its disk. The bucket is intentionally retained by CloudFormation; separately remove its objects and the bucket only after verifying the local Gold backup.

## Validation and approval boundary

AWS CloudFormation `validate-template` succeeded. This validates the template but does not prove resource-creation permissions, capacity availability, bootstrap success, or the runtime stop timer. Those checks happen after approved deployment.

Deployment requires `CAPABILITY_IAM` because it creates a dedicated instance role. No existing role or networking resources will be changed.
