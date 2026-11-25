"""Silver-only processing. Readers MUST resolve manifests/silver/...current.json.

One writer per dataset/partition (enforced by the serial Airflow DAG). Attempts
are immutable; a failed attempt never replaces the committed manifest. No Gold.
"""

import argparse
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

VERSION = "silver-v1"
CODE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
SCHEMAS = {
    "pod": {
        "pod_id": "string",
        "workload_id": "string",
        "server_id": "string",
        "cluster_id": "string",
        "state_public": "string",
        "gpu_spec_public": "string",
        "server_gpu_count": "bigint",
        "server_cpu_capacity_cores": "bigint",
        "priority_class": "string",
        "job_type_public": "string",
        "model_type_public": "string",
        "is_genai_request": "boolean",
        "gpu_request": "double",
        "gpu_mem_request": "bigint",
        "cpu_request_cores": "double",
        "used_gpu_hours": "double",
        "avg_gpu_sm_util": "double",
        "avg_gpu_mem_gib": "double",
        "avg_cpu_request_util": "double",
        "avg_memory_util": "double",
        "ready_status": "boolean",
        "schedule_delay_sec": "bigint",
        "ready_delay_sec": "bigint",
    },
    "server": {
        "server_id": "string",
        "cluster_id": "string",
        "asw_id": "string",
        "gpu_spec_public": "string",
        "gpu_count": "bigint",
        "cpu_capacity_cores": "bigint",
    },
    "network": {"server_id": "string", "rx_gibps_avg": "double", "tx_gibps_avg": "double"},
    "execution": {
        "pod_id": "string",
        "workload_id": "string",
        "server_id": "string",
        "gpu_spec_public": "string",
        "priority_class": "string",
        "job_type_public": "string",
        "model_type_public": "string",
        "is_genai_request": "boolean",
        "gpu_request": "double",
        "duration_hours": "double",
        "schedule_delay_sec": "bigint",
        "ready_delay_sec": "bigint",
        "ready_status": "boolean",
        "schedule_status": "boolean",
    },
}


class Store:
    def __init__(self, root):
        self.root = root.rstrip("/")
        self.remote = self.root.startswith("s3://")
        if self.remote:
            import boto3

            parsed = urlparse(self.root)
            self.bucket, self.prefix = parsed.netloc, parsed.path.strip("/")
            self.s3 = boto3.client("s3")

    def key(self, relative):
        return "/".join(x for x in (self.prefix, relative) if x)

    def uri(self, relative, spark=False):
        result = self.root + "/" + relative
        return result.replace("s3://", "s3a://", 1) if spark else result

    def read(self, relative):
        if self.remote:
            from botocore.exceptions import ClientError

            try:
                return json.loads(
                    self.s3.get_object(Bucket=self.bucket, Key=self.key(relative))["Body"].read()
                )
            except ClientError as error:
                if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                    return None
                raise
        path = Path(self.uri(relative))
        return json.loads(path.read_text()) if path.exists() else None

    def write(self, relative, value):
        body = json.dumps(value, indent=2, allow_nan=False).encode()
        if self.remote:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key(relative),
                Body=body,
                ContentType="application/json",
            )
        else:
            path = Path(self.uri(relative))
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
            temp.write_bytes(body)
            temp.replace(path)

    def footer(self, relative):
        import pyarrow.parquet as pq

        if self.remote:
            import pyarrow.fs as fs

            if not hasattr(self, "arrow_fs"):
                self.arrow_fs = fs.S3FileSystem(
                    region=os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
                )
            return pq.ParquetFile(self.bucket + "/" + self.key(relative), filesystem=self.arrow_fs)
        return pq.ParquetFile(self.uri(relative))

    def files(self, relative):
        if self.remote:
            prefix = self.key(relative).rstrip("/") + "/"
            result = []
            for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix
            ):
                for item in page.get("Contents", []):
                    if item["Key"].endswith(".parquet"):
                        result.append(
                            {
                                "path": item["Key"][len(self.prefix) + 1 :]
                                if self.prefix
                                else item["Key"],
                                "bytes": item["Size"],
                                "identity": item["ETag"],
                            }
                        )
            return sorted(result, key=lambda x: x["path"])
        return [
            {
                "path": str(p.relative_to(self.root)),
                "bytes": p.stat().st_size,
                "identity": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for p in sorted(Path(self.uri(relative)).rglob("*.parquet"))
        ]


def partition(dataset, day):
    if dataset not in SCHEMAS or not 0 <= day <= 184:
        raise ValueError("Invalid dataset/day")
    if dataset == "network" and not 109 <= day <= 115:
        raise ValueError("Network only covers days 109–115")
    return "all" if dataset == "execution" else f"day={day}"


def source_observations(store, relative):
    catalog = store.read("manifests/bronze/quality-observations.json") or {}
    return sorted(
        [
            row
            for row in catalog.get("observations", [])
            if row["source_key"].startswith(relative + "/")
        ],
        key=lambda row: (row["source_key"], row.get("code", "")),
    )


def source_inventory(store, dataset, day):
    relative = f"bronze/{dataset}/{partition(dataset, day)}"
    marker = store.read(f"manifests/bronze/{dataset}.success.json")
    if marker is None:
        raise ValueError("Verified Bronze success marker required")
    files = store.files(relative)
    if not files:
        raise ValueError("No Bronze Parquet files")
    inventory = store.read(f"manifests/bronze/{dataset}.json")
    if inventory is None:
        raise ValueError("Bronze source inventory required")
    expected = {
        x["key"]: x["bytes"] for x in inventory["files"] if x["key"].startswith(relative + "/")
    }
    if {x["path"]: x["bytes"] for x in files} != expected:
        raise ValueError("Bronze partition differs from verified inventory")
    if dataset != "execution":
        hours = {int(x["path"].split("/hour=")[1].split("/")[0]) for x in files}
        if hours != set(range(24)):
            raise ValueError(f"Expected 24 hours; found {sorted(hours)}")
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "marker": marker,
                "files": files,
                "observations": source_observations(store, relative),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return relative, files, fingerprint


ARROW_TYPES = {
    "string": "string",
    "bool": "boolean",
    "int64": "bigint",
    "int32": "int",
    "float": "float",
    "double": "double",
}


def compatible_schema(dataset, actual):
    allowed = {
        "double": {"double", "float", "bigint", "int", "null"},
        "bigint": {"bigint", "int", "null"},
        "string": {"string", "null"},
        "boolean": {"boolean", "null"},
    }
    return set(actual) == set(SCHEMAS[dataset]) and all(
        actual[name] in allowed[expected] for name, expected in SCHEMAS[dataset].items()
    )


def read_source(spark, store, dataset, relative, files):
    groups = {}
    for item in files:
        schema = store.footer(item["path"]).schema_arrow
        actual = {field.name: ARROW_TYPES.get(str(field.type), str(field.type)) for field in schema}
        if not compatible_schema(dataset, actual):
            raise ValueError(f"Source schema drift in {item['path']}: {actual}")
        signature = tuple((name, actual[name]) for name in SCHEMAS[dataset])
        groups.setdefault(signature, []).append(store.uri(item["path"], spark=True))
    data = None
    for signature, paths in groups.items():
        actual = dict(signature)
        # Read the native physical type; Spark cannot directly read INT64 as double
        # or a Parquet null-only INT32 as string. Normalize only after reading.
        schema = T.StructType(
            [
                T.StructField(name, T._parse_datatype_string(dtype), True)
                for name, dtype in signature
                if dtype != "null"
            ]
        )
        frame = (
            spark.read.schema(schema)
            .option("basePath", store.uri(relative, spark=True))
            .parquet(*paths)
        )
        wide = [
            name
            for name, expected in SCHEMAS[dataset].items()
            if expected == "double" and actual[name] == "bigint"
        ]
        if wide:
            invalid = F.lit(False)
            for name in wide:
                invalid = invalid | (F.col(name) < -(2**53)) | (F.col(name) > 2**53)
            if frame.filter(invalid).limit(1).count():
                raise ValueError("Integer-to-double widening would exceed the exact integer range")
        for name, expected in SCHEMAS[dataset].items():
            value = F.lit(None) if actual[name] == "null" else F.col(name)
            frame = frame.withColumn(name, value.cast(expected))
        frame = frame.withColumn("source_file", F.input_file_name())
        data = frame if data is None else data.unionByName(frame)
    actual = {
        f.name: f.dataType.simpleString()
        for f in data.schema
        if f.name not in ("hour", "source_file")
    }
    if actual != SCHEMAS[dataset]:
        raise ValueError(f"Source schema drift: expected {SCHEMAS[dataset]}, got {actual}")
    if dataset != "execution" and data.schema["hour"].dataType.simpleString() not in (
        "int",
        "bigint",
    ):
        raise ValueError("Hour must be an integer partition")
    return data


def transform(data, dataset, day, fingerprint):
    if "source_file" not in data.columns:
        data = data.withColumn("source_file", F.input_file_name())
    if dataset != "execution":
        data = data.withColumn("day", F.lit(day).cast("int")).withColumn(
            "hour", F.col("hour").cast("int")
        )
    data = data.withColumn("transformation_version", F.lit(VERSION)).withColumn(
        "source_fingerprint", F.lit(fingerprint)
    )
    flags = []
    for name, dtype in SCHEMAS[dataset].items():
        col = F.col(name)
        if dtype in ("double", "bigint"):
            if dtype == "double":
                flags.append(
                    F.when(F.isnan(col) | (F.abs(col) == float("inf")), F.lit(name + ":non_finite"))
                )
            flags.append(F.when(col < 0, F.lit(name + ":negative")))
        if dtype == "string" and name.endswith("_public"):
            flags.append(
                F.when(
                    col.isNull() | (F.trim(col) == "") | (F.lower(col) == "unknown"),
                    F.lit(name + ":unknown"),
                )
            )
    if "gpu_request" in data.columns:
        data = data.withColumn(
            "is_gpu_requesting",
            F.when(
                F.col("gpu_request").isNull()
                | F.isnan("gpu_request")
                | (F.abs(F.col("gpu_request")) == float("inf")),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("gpu_request") > 0),
        )
    if "avg_gpu_sm_util" in data.columns:
        data = data.withColumn(
            "has_sm_measurement",
            F.col("avg_gpu_sm_util").isNotNull()
            & ~F.isnan("avg_gpu_sm_util")
            & (F.abs(F.col("avg_gpu_sm_util")) != float("inf")),
        )
    for name in ("ready_delay_sec", "schedule_delay_sec"):
        if name in data.columns:
            data = data.withColumn(name + "_is_negative", F.coalesce(F.col(name) < 0, F.lit(False)))
    reasons = []
    if dataset != "execution":
        reasons.append(
            F.when(F.col("hour").isNull() | ~F.col("hour").between(0, 23), F.lit("invalid_hour"))
        )
    if dataset in ("server", "network"):
        reasons.append(
            F.when(
                F.col("server_id").isNull() | (F.trim("server_id") == ""),
                F.lit("missing_server_key"),
            )
        )
        count = F.count("*").over(Window.partitionBy("day", "hour", "server_id"))
        reasons.append(F.when(count > 1, F.lit("duplicate_server_hour_key")))
    # Workload/execution candidate keys are audited, not silently deduplicated.
    data = data.withColumn("quality_flags", F.filter(F.array(*flags), lambda x: x.isNotNull()))
    data = data.withColumn(
        "quarantine_reasons",
        F.filter(F.array(*reasons).cast("array<string>"), lambda x: x.isNotNull()),
    )
    return data


def quality_report(data, dataset):
    expressions = [
        F.count("*").alias("input_rows"),
        F.sum(F.when(F.size("quarantine_reasons") == 0, 1).otherwise(0)).alias("accepted_rows"),
    ]
    names = list(SCHEMAS[dataset])
    expressions += [
        F.sum(F.col(name).isNull().cast("long")).alias(f"null_{i}") for i, name in enumerate(names)
    ]
    string_names = [n for n, t in SCHEMAS[dataset].items() if t == "string"]
    double_names = [n for n, t in SCHEMAS[dataset].items() if t == "double"]
    bool_names = [n for n, t in SCHEMAS[dataset].items() if t == "boolean"]
    for i, name in enumerate(string_names):
        expressions += [
            F.sum((F.trim(F.col(name)) == "").cast("long")).alias(f"blank_{i}"),
            F.sum((F.lower(F.trim(F.col(name))) == "unknown").cast("long")).alias(f"unknown_{i}"),
        ]
    for i, name in enumerate(double_names):
        expressions += [
            F.sum(F.isnan(name).cast("long")).alias(f"nan_{i}"),
            F.sum((F.col(name) == float("inf")).cast("long")).alias(f"posinf_{i}"),
            F.sum((F.col(name) == -float("inf")).cast("long")).alias(f"neginf_{i}"),
        ]
    for i, name in enumerate(bool_names):
        expressions.append(F.sum((F.col(name) == F.lit(True)).cast("long")).alias(f"true_{i}"))
    values = data.agg(*expressions).first().asDict()
    report = {k: int(values[k] or 0) for k in ("input_rows", "accepted_rows")}
    report["quarantined_rows"] = report["input_rows"] - report["accepted_rows"]
    report["null_counts"] = {name: int(values[f"null_{i}"] or 0) for i, name in enumerate(names)}
    report["blank_counts"] = {
        name: int(values[f"blank_{i}"] or 0) for i, name in enumerate(string_names)
    }
    report["unknown_literal_counts"] = {
        name: int(values[f"unknown_{i}"] or 0) for i, name in enumerate(string_names)
    }
    report["non_finite_counts"] = {
        name: {
            label: int(values[f"{key}_{i}"] or 0)
            for key, label in [
                ("nan", "nan"),
                ("posinf", "positive_infinity"),
                ("neginf", "negative_infinity"),
            ]
        }
        for i, name in enumerate(double_names)
    }
    report["boolean_counts"] = {
        name: {
            "true": int(values[f"true_{i}"] or 0),
            "false": report["input_rows"]
            - report["null_counts"][name]
            - int(values[f"true_{i}"] or 0),
            "null": report["null_counts"][name],
        }
        for i, name in enumerate(bool_names)
    }
    for column in ("quality_flags", "quarantine_reasons"):
        report[column] = {
            r["flag"]: r["count"]
            for r in data.select(F.explode(column).alias("flag")).groupBy("flag").count().collect()
        }
    keys = (
        ["day", "hour", "pod_id", "server_id"]
        if dataset == "pod"
        else (["pod_id"] if dataset == "execution" else ["day", "hour", "server_id"])
    )
    duplicate = (
        data.groupBy(*keys)
        .count()
        .filter("count > 1")
        .agg(F.count("*").alias("groups"), F.sum("count").alias("rows"))
        .first()
    )
    report["candidate_key_audit"] = {
        "keys": keys,
        "duplicate_groups": duplicate["groups"],
        "rows_in_duplicate_groups": duplicate["rows"] or 0,
        "policy": "quarantine_all_duplicate_keys"
        if dataset in ("server", "network")
        else "retain_and_report_unproven_key",
    }
    return report


def audit_inventory(spark, store, dataset, day, data):
    if dataset not in ("pod", "network"):
        return {"status": "not_applicable"}
    manifest = store.read(f"manifests/silver/server/day={day}.current.json")
    if not manifest or manifest["version"] != VERSION:
        return {"status": "pending_server_silver", "decision_ready": False}
    # Only committed accepted inventory keys; never fan out server capacity.
    inventory = spark.read.parquet(store.uri(manifest["accepted"], spark=True))
    keys = ["day", "hour", "server_id"]
    observed = data.filter(F.size("quarantine_reasons") == 0).select(*keys)
    known = inventory.select(*keys)
    matched = observed.join(known, keys, "left_semi").count()
    unmatched = observed.join(known, keys, "left_anti").count()
    unobserved = known.join(observed.select(*keys).distinct(), keys, "left_anti").count()
    return {
        "status": "complete",
        "inventory_source_fingerprint": manifest["source_fingerprint"],
        "inventory_manifest": f"manifests/silver/server/day={day}.current.json",
        "matched_observations": matched,
        "unmatched_observations": unmatched,
        "inventory_server_hours_without_observations": unobserved,
        "inventory_quarantined_rows": manifest["quality"]["quarantined_rows"],
        "interpretation": "Unmatched/missing observations are unknown, not zero demand.",
    }


def run(spark, store, dataset, day, force=False, output_partitions=4):
    start = time.monotonic()
    part = partition(dataset, day)
    current_key = f"manifests/silver/{dataset}/{part}.current.json"
    relative, files, fingerprint = source_inventory(store, dataset, day)
    previous = store.read(current_key)
    if (
        not force
        and previous
        and previous["source_fingerprint"] == fingerprint
        and previous["version"] == VERSION
        and previous.get("code_sha256") == CODE_SHA256
    ):
        # Detect deleted/truncated output before declaring a successful resume.
        if all(
            store.files(previous[area]) == previous["output_files"][area]
            for area in ("accepted", "quarantine")
        ):
            return dict(previous, skipped=True)
    attempt = f"silver_versions/{dataset}/{part}/{VERSION}/{uuid.uuid4().hex}"
    data = transform(
        read_source(spark, store, dataset, relative, files), dataset, day, fingerprint
    ).persist(StorageLevel.DISK_ONLY)
    try:
        report = quality_report(data, dataset)
        report["source_observations"] = source_observations(store, relative)
        if report["input_rows"] == 0:
            raise ValueError("Empty source partition")
        output_files = {}
        for area, condition, expected in [
            ("accepted", F.size("quarantine_reasons") == 0, report["accepted_rows"]),
            ("quarantine", F.size("quarantine_reasons") > 0, report["quarantined_rows"]),
        ]:
            path = attempt + "/" + area
            data.filter(condition).repartition(output_partitions).write.option(
                "compression", "zstd"
            ).parquet(store.uri(path, spark=True))
            if spark.read.parquet(store.uri(path, spark=True)).count() != expected:
                raise ValueError("Output row reconciliation failed")
            output_files[area] = store.files(path)
            if (
                sum(store.footer(item["path"]).metadata.num_rows for item in output_files[area])
                != expected
            ):
                raise ValueError("Independent Arrow footer reconciliation failed")
        report["inventory_join_audit"] = audit_inventory(spark, store, dataset, day, data)
        # Refuse a commit if Bronze changed during processing.
        if source_inventory(store, dataset, day)[2] != fingerprint:
            raise ValueError("Bronze changed during transformation")
        result = {
            "dataset": dataset,
            "day": None if dataset == "execution" else day,
            "synthetic_source": bool(
                store.read(f"manifests/bronze/{dataset}.success.json").get("synthetic", False)
            ),
            "version": VERSION,
            "code_sha256": CODE_SHA256,
            "source_fingerprint": fingerprint,
            "source_bytes": sum(x["bytes"] for x in files),
            "accepted": attempt + "/accepted",
            "quarantine": attempt + "/quarantine",
            "quality": report,
            "output_files": output_files,
            "elapsed_seconds": time.monotonic() - start,
            "spark_application_id": spark.sparkContext.applicationId,
            "spark_master": spark.sparkContext.master,
            "output_partitions": output_partitions,
            "gpu_view_filter": "is_gpu_requesting = true" if dataset == "pod" else None,
        }
        store.write(attempt + "/report.json", result)
        store.write(current_key, result)  # Atomic object PUT / local rename; commit LAST.
        return result
    finally:
        data.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=SCHEMAS)
    parser.add_argument("day", type=int)
    parser.add_argument("--root", required=True, help="Local test root or s3://bucket[/prefix]")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--output-partitions", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Audit committed Silver against committed server inventory; no transformation",
    )
    parser.add_argument(
        "--event-log-dir", help="Existing file:// or s3a:// directory for Spark event logs"
    )
    args = parser.parse_args()
    partition(args.dataset, args.day)
    if args.output_partitions < 1:
        parser.error("output-partitions must be positive")
    store = Store(args.root)
    builder = (
        SparkSession.builder.appName(f"silver-{args.dataset}-{args.day}")
        .master(args.master)
        .config("spark.sql.shuffle.partitions", "16")
    )
    if store.remote:
        builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    if args.event_log_dir:
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", args.event_log_dir)
            .config("spark.eventLog.logStageExecutorMetrics", "true")
        )
    spark = builder.getOrCreate()
    try:
        if args.audit_only:
            current = store.read(
                f"manifests/silver/{args.dataset}/{partition(args.dataset, args.day)}.current.json"
            )
            if not current:
                raise ValueError("Committed Silver required")
            data = spark.read.parquet(store.uri(current["accepted"], spark=True))
            audit = audit_inventory(spark, store, args.dataset, args.day, data)
            if audit["status"] == "pending_server_silver":
                raise ValueError("Committed server Silver required for join audit")
            audit["silver_attempt"] = current["accepted"]
            store.write(
                f"manifests/silver_audits/{args.dataset}/{partition(args.dataset, args.day)}.json",
                audit,
            )
            print(json.dumps(audit, indent=2))
        else:
            print(
                json.dumps(
                    run(spark, store, args.dataset, args.day, args.force, args.output_partitions),
                    indent=2,
                )
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
