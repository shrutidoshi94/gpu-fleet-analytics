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


def process(dataset, day, archive, bucket):
    table = f"asi_opensource_{dataset}_hourly"
    prefix = f"s3://{bucket}" if bucket else None
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        bronze = root / "bronze"
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
            key = f"{layer}/{dataset}/day={day}"
            if prefix:
                subprocess.run(["aws", "s3", "sync", str(path), f"{prefix}/{key}/", "--delete"], check=True)
            else:
                dest = Path("output") / key
                shutil.rmtree(dest, ignore_errors=True)
                shutil.copytree(path, dest)

        publish("bronze", bronze)
        spark = SparkSession.builder.appName(f"{dataset}-day-{day}").getOrCreate()
        try:
            data = spark.read.parquet(str(bronze))
            if dataset == "pod":
                silver = data.filter(F.col("gpu_request") > 0).select(
                    "hour", "pod_id", "server_id", "job_type_public", "gpu_request", "avg_gpu_sm_util")
                groups = ["job_type_public"]
                metrics = [F.avg("avg_gpu_sm_util").alias("mean_sm_util"),
                           F.count("avg_gpu_sm_util").alias("measured_pod_hours")]
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
                {"dataset": dataset, "day": day, "rows": [r.asDict() for r in rows]}, indent=2))
            publish("gold", gold)
        finally:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["network", "pod"])
    parser.add_argument("day", type=int, choices=range(185), metavar="DAY")
    args = parser.parse_args()
    archive = Path(os.environ.get("TRACE_DIR", ".")) / f"asi_opensource_{args.dataset}_hourly.zip"
    process(args.dataset, args.day, archive, os.environ.get("TRACE_BUCKET"))
