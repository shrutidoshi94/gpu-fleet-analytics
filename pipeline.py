"""Process one complete Alibaba trace day using local Spark on an EC2 worker."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from zipfile import ZipFile

from pyspark.sql import SparkSession, functions as F


def process(dataset, day, archive, bucket, from_bronze=False):
    table = f"asi_opensource_{dataset}_hourly"
    prefix = f"s3://{bucket}" if bucket else None
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        bronze = root / "bronze"
        if not from_bronze:
            hours = set()
            with ZipFile(archive) as source:
                for name in source.namelist():
                    match = re.fullmatch(rf"(?:.*/)?{table}/day=0*{day}/hour=(\d{{1,2}})/([^/]+\.parquet)", name)
                    if match:
                        hour, filename = match.groups()
                        target = bronze / f"hour={int(hour):02}" / filename
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with source.open(name) as reader, target.open("wb") as writer:
                            shutil.copyfileobj(reader, writer)
                        hours.add(int(hour))
            if hours != set(range(24)):
                raise ValueError(f"Expected 24 hours for day {day}; found {sorted(hours)}")
        def publish(layer, path):
            partition = "all" if dataset == "execution" else f"day={day}"
            key = f"{layer}/{dataset}/{partition}"
            if prefix:
                subprocess.run(["aws", "s3", "sync", str(path), f"{prefix}/{key}/", "--delete"], check=True)
            else:
                dest = Path("output") / key
                shutil.rmtree(dest, ignore_errors=True)
                shutil.copytree(path, dest)

        builder = SparkSession.builder.appName(f"{dataset}-day-{day}")
        if from_bronze:
            if not bucket:
                raise ValueError("TRACE_BUCKET is required when reading S3 Bronze")
            # Require complete ingestion before consuming any dataset partition.
            subprocess.run(["aws", "s3api", "head-object", "--bucket", bucket,
                            "--key", f"manifests/bronze/{dataset}.success.json"],
                           check=True, stdout=subprocess.DEVNULL)
            partition = "all" if dataset == "execution" else f"day={day}"
            bronze_source = f"s3a://{bucket}/bronze/{dataset}/{partition}"
            builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        else:
            publish("bronze", bronze)
            bronze_source = str(bronze)
        spark = builder.getOrCreate()
        try:
            data = spark.read.parquet(bronze_source)
            if dataset == "pod":
                silver = data.filter(F.col("gpu_request") > 0).select(
                    "hour", "pod_id", "server_id", "job_type_public", "gpu_request", "avg_gpu_sm_util")
                groups = ["job_type_public"]
                metrics = [F.avg("avg_gpu_sm_util").alias("mean_sm_util"),
                           F.count("avg_gpu_sm_util").alias("measured_pod_hours")]
            elif dataset == "server":
                silver = data.filter(F.col("server_id").isNotNull())
                groups = ["gpu_spec_public"]
                metrics = [F.sum("gpu_count").alias("observed_gpu_hours"),
                           F.avg("gpu_count").alias("mean_gpus_per_server"),
                           F.countDistinct("server_id").alias("distinct_servers")]
            elif dataset == "execution":
                silver = data.filter((F.col("gpu_request") > 0) & (F.col("duration_hours") >= 0))
                groups = ["job_type_public"]
                metrics = [F.avg("duration_hours").alias("mean_duration_hours"),
                           F.expr("percentile_approx(duration_hours, 0.5)").alias("median_duration_hours"),
                           F.expr("percentile_approx(duration_hours, 0.95)").alias("p95_duration_hours")]
            else:
                silver = data.select("hour", "server_id", "rx_gibps_avg", "tx_gibps_avg")
                groups = ["hour"]
                metrics = [F.avg("rx_gibps_avg").alias("mean_rx_gibps"),
                           F.avg("tx_gibps_avg").alias("mean_tx_gibps")]
            silver.write.parquet(str(root / "silver"))
            publish("silver", root / "silver")
            rows = spark.read.parquet(str(root / "silver")).groupBy(*groups).agg(
                F.count("*").alias("observations"), *metrics).orderBy(*groups).collect()
            gold = root / "gold"
            gold.mkdir()
            (gold / "summary.json").write_text(json.dumps(
                {"dataset": dataset, "day": None if dataset == "execution" else day, "rows": [r.asDict() for r in rows]}, indent=2))
            publish("gold", gold)
        finally:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["network", "pod", "server", "execution"])
    parser.add_argument("day", type=int, choices=range(185), metavar="DAY")
    parser.add_argument("--from-bronze", action="store_true", help="Read verified S3 Bronze using S3A; do not read local ZIPs")
    args = parser.parse_args()
    if args.dataset in ("server", "execution") and not args.from_bronze:
        parser.error("server and execution currently require --from-bronze")
    if args.dataset == "network" and not 109 <= args.day <= 115:
        parser.error("network days must be 109 through 115")
    archive = Path(os.environ.get("TRACE_DIR", ".")) / f"asi_opensource_{args.dataset}_hourly.zip"
    process(args.dataset, args.day, archive, os.environ.get("TRACE_BUCKET"), args.from_bronze)
