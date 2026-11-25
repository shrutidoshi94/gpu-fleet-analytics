"""Independently verify Gold group counts and means against S3 Bronze with PyArrow."""

import argparse
import json
import math
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq


def verify(dataset, day, bucket):
    partition = "all" if dataset == "execution" else f"day={day}"
    prefix = f"s3://{bucket}"
    report = json.loads(
        subprocess.check_output(
            ["aws", "s3", "cp", f"{prefix}/gold/{dataset}/{partition}/summary.json", "-"]
        )
    )
    expected = defaultdict(lambda: {"observations": 0, "sum": 0.0, "measured": 0, "servers": set()})
    columns = {
        "network": ["server_id", "rx_gibps_avg", "tx_gibps_avg"],
        "server": ["server_id", "gpu_spec_public", "gpu_count"],
        "pod": ["job_type_public", "gpu_request", "avg_gpu_sm_util"],
        "execution": ["job_type_public", "gpu_request", "duration_hours"],
    }[dataset]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"{prefix}/bronze/{dataset}/{partition}/",
                str(root),
                "--only-show-errors",
            ],
            check=True,
        )
        for path in root.rglob("*.parquet"):
            hour = int(path.parent.name.split("=")[1]) if dataset == "network" else None
            for batch in pq.ParquetFile(path).iter_batches(batch_size=16384, columns=columns):
                for row in batch.to_pylist():
                    if dataset in ("pod", "execution") and not (
                        row["gpu_request"] is not None and row["gpu_request"] > 0
                    ):
                        continue
                    if dataset == "execution" and not (
                        row["duration_hours"] is not None and row["duration_hours"] >= 0
                    ):
                        continue
                    if dataset == "server" and row["server_id"] is None:
                        continue
                    key = (
                        hour
                        if dataset == "network"
                        else row["gpu_spec_public" if dataset == "server" else "job_type_public"]
                    )
                    group = expected[key]
                    group["observations"] += 1
                    field = {
                        "pod": "avg_gpu_sm_util",
                        "server": "gpu_count",
                        "execution": "duration_hours",
                        "network": "rx_gibps_avg",
                    }[dataset]
                    value = row[field]
                    if value is not None:
                        group["sum"] += value
                        group["measured"] += 1
                    if dataset == "server":
                        group["servers"].add(row["server_id"])
        silver_root = root / "_silver_validation"
        subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"{prefix}/silver/{dataset}/{partition}/",
                str(silver_root),
                "--only-show-errors",
            ],
            check=True,
        )
        silver_rows = sum(
            pq.ParquetFile(path).metadata.num_rows for path in silver_root.rglob("*.parquet")
        )
        assert silver_rows == sum(group["observations"] for group in expected.values()), (
            dataset,
            silver_rows,
        )
        keyfield = {
            "network": "hour",
            "server": "gpu_spec_public",
            "pod": "job_type_public",
            "execution": "job_type_public",
        }[dataset]
        assert len(report["rows"]) == len(expected)
        for row in report["rows"]:
            group = expected[row[keyfield]]
            assert row["observations"] == group["observations"], (
                dataset,
                row,
                group["observations"],
            )
            mean = group["sum"] / group["measured"] if group["measured"] else None
            metric = {
                "network": "mean_rx_gibps",
                "server": "mean_gpus_per_server",
                "pod": "mean_sm_util",
                "execution": "mean_duration_hours",
            }[dataset]
            actual = row[metric]
            assert (mean is None and actual is None) or math.isclose(
                mean, actual, rel_tol=1e-8, abs_tol=1e-10
            ), (dataset, row, mean)
            if dataset == "server":
                assert math.isclose(row["observed_gpu_hours"], group["sum"], rel_tol=1e-8)
                assert row["distinct_servers"] == len(group["servers"])
            if dataset == "pod":
                assert row["measured_pod_hours"] == group["measured"]
    result = {
        "dataset": dataset,
        "day": None if dataset == "execution" else day,
        "groups": len(expected),
        "observations": sum(x["observations"] for x in expected.values()),
        "verification": "PyArrow source grouping, counts and means against S3 Bronze",
    }
    print(json.dumps(result), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["network", "server", "pod", "execution"])
    parser.add_argument("day", type=int)
    parser.add_argument("--bucket", required=True)
    a = parser.parse_args()
    result = verify(a.dataset, a.day, a.bucket)
    Path(f"logs/verified-gold-{a.dataset}.json").write_text(json.dumps(result, indent=2))
