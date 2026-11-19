# GPU fleet analytics

Explore Alibaba GPU fleet telemetry one trace day at a time. The network report covers days 109–115. The workload report covers days 0–184 and compares observed pod utilization by workload type. Trace days are relative to the start of recording.

## Run a local demonstration

Use Python 3.11 and Java 17. From the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
unset SPARK_HOME  # use the PySpark version installed in this environment
export PYSPARK_PYTHON="$(command -v python)"
python examples/make_sample.py
TRACE_DIR=sample python pipeline.py network 109
streamlit run dashboard.py
# Verify metrics, day replacement, incomplete-day rejection, and offline dashboard:
python -m unittest discover -s tests -v
```

On macOS with Homebrew Java 17, set `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`. If Spark cannot resolve your host, set `SPARK_LOCAL_IP=127.0.0.1`. A system `SPARK_HOME` pointing at another Spark release causes a Java/Python version mismatch.

The generated network trace is synthetic. GitHub Actions runs this example and exports the Gold JSON. A real run can use the same dashboard after the infrastructure is removed by uploading its saved summary.

## Run on EC2

Use a Linux EC2 worker with Java 17, Python 3.11, AWS CLI, a bucket in the same region, and an instance role scoped to list the target bucket and read, write and delete project objects. Use a dedicated bucket or workspace because sync replaces files within each selected layer/dataset/day prefix. Allow local disk for the ZIP plus a day of extracted data, Silver, and Spark scratch space. Set TMPDIR and SPARK_LOCAL_DIRS to the data volume if necessary.

```bash
export TRACE_PROJECT="$PWD" TRACE_DIR="$PWD/archives"
export TRACE_BUCKET=your-project-bucket
mkdir -p "$TRACE_DIR"
table=asi_opensource_network_hourly
curl --fail --location --continue-at - --output "$TRACE_DIR/$table.zip" \
  "https://tre-clusterdata.oss-cn-hangzhou.aliyuncs.com/cluster-trace-gpu-v2026/data/$table.zip"
python pipeline.py network 109
```

Start with the approximately 204 MB network archive and day 109. No GPU instance is needed to analyze this telemetry.

For workload data, set table to asi_opensource_pod_hourly before downloading, then run python pipeline.py pod 0. The workload ZIP is about 352 GB. Reuse it across runs. Each run extracts just the selected day, publishes its Parquet files under Bronze, transforms them on the worker, and publishes Silver and then Gold to S3. Spark reads the staged local files on this single-node implementation. A retry replaces the day. S3 updates across the three prefixes are sequential, so consume Gold after task success.

## Orchestrate with Airflow

In the same Python 3.11 environment on the worker, install Airflow and start a local demo scheduler with the repository DAG folder:

```bash
pip install 'apache-airflow==3.0.6' --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.11.txt
export TRACE_PROJECT="$PWD" AIRFLOW_HOME="$PWD/airflow-home" AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
# Optional one-shot verification before starting the scheduler:
airflow db migrate
airflow dags test gpu_trace_day 2026-01-01
airflow standalone
```

Streamlit is pinned to 1.48.1 to remain compatible with Airflow 3.0.6 constraints. Install the project requirements first, then Airflow with its constraints as shown; run `pip check` afterward.

Keep TRACE_DIR and TRACE_BUCKET set in the scheduler environment. Trigger gpu_trace_day from the UI with dataset and day. For the local synthetic run use TRACE_DIR pointing to the absolute sample directory and leave TRACE_BUCKET unset. The DAG serializes runs to avoid competing writes. Download the Gold summary before removing AWS resources.

## Source

[Alibaba GPU Cluster Trace v2026, schema, archive sizes and paper](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-gpu-v2026). Cite Li et al., *Heterogeneity at Hyperscale*, OSDI 2026, when using the trace. Mean SM utilization gives each non-null pod-hour equal weight. It is reported in source units and does not estimate the fraction of the whole fleet in use.

## Keep the dashboard after AWS teardown

After a successful S3 run, download the small Gold file to your laptop **before deleting the bucket**:

```bash
mkdir -p output/gold/network/day=109
aws s3 cp "s3://$TRACE_BUCKET/gold/network/day=109/summary.json" \
  output/gold/network/day=109/summary.json
streamlit run dashboard.py
```

The dashboard reads only Gold JSON; it needs no AWS credentials, Spark, source archive, or running worker. Keep a separate backup of this JSON (the `output/` directory is gitignored). You can also upload it through the dashboard's file picker.

## First AWS run scope

Use one short-lived CPU EC2 worker for Airflow and Spark, one private S3 bucket in the same region, and an instance role scoped to that bucket. Start with 2 vCPU / 8 GiB RAM and a 30 GiB gp3 volume, then measure actual memory and disk use for network day 109. Use a security group with no public application ports; access via Session Manager and a tunnel if needed. Use an existing suitable VPC/subnet. Avoid a NAT gateway, managed Airflow, EMR, and GPU instances for this small first run.

Confirm the AWS profile, region, networking, and current regional pricing before provisioning. Set a small project budget alert; an alert does not cap spending. Stop the worker after the run and delete its EBS volume when no longer needed. Save and verify Gold locally before deleting S3 data. The 352 GB workload archive is deferred and needs a separate storage and cost plan.

## Resume this project

Read [HANDOFF.md](HANDOFF.md) for verified results, AWS resource IDs, runtime setup, and the exact continuation and cleanup steps. The first real network day (109) and its rerun passed; the worker is stopped and the saved Gold runs locally.
