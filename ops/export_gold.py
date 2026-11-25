"""Export verified, compact Gold Parquet plus lineage; no AWS needed to read bundle."""

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.gold import publish
from pipelines.silver import Store


def export(store, release, destination):
    plan = store.read(f"manifests/gold/{release}/plan.json")
    published = publish(store, plan)
    destination = Path(destination)
    if destination.exists():
        metadata = json.loads((destination / "bundle.json").read_text())
        if metadata["plan_identity"] != plan["identity"]:
            raise ValueError("Existing bundle belongs to another release")
        for name, value in metadata["files"].items():
            if (
                Path(name).name != name
                or hashlib.sha256((destination / name).read_bytes()).hexdigest() != value["sha256"]
            ):
                raise ValueError("Existing bundle failed integrity validation")
        if store.remote:
            for path in destination.iterdir():
                store.s3.upload_file(
                    str(path), store.bucket, store.key(f"gold_exports/{release}/{path.name}")
                )
            store.write(f"manifests/gold/{release}/export.json", metadata)
        return metadata
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="gold-export-", dir=destination.parent))
    try:
        tables = {}
        for unit in published["units"].values():
            for name, table in unit["tables"].items():
                tables.setdefault(name, []).extend(table["files"])
        metadata = {
            "release": release,
            "plan_identity": plan["identity"],
            "code_sha256": plan["code_sha256"],
            "relative_days": plan["days"],
            "execution_scope": "global",
            "network_scope": "days109–115 only",
            "metric_policy": "observed request/activity evidence; no automatic purchase or reclaim recommendation",
            "tables": {},
            "files": {},
        }
        for name, files in tables.items():
            rows = []
            for i, item in enumerate(files):
                if not item["path"].endswith(".parquet"):
                    continue
                if store.remote:
                    local = stage / f".download-{i}.parquet"
                    store.s3.download_file(store.bucket, store.key(item["path"]), str(local))
                else:
                    local = Path(store.uri(item["path"]))
                rows.append(pq.ParquetFile(local).read())
                if store.remote:
                    local.unlink()
            combined = pa.concat_tables(rows, promote_options="default")
            path = stage / (name + ".parquet")
            pq.write_table(combined, path, compression="zstd")
            metadata["tables"][name] = {"rows": combined.num_rows, "columns": combined.column_names}
        (stage / "lineage.json").write_text(
            json.dumps({"plan": plan, "release": published}, indent=2)
        )
        (stage / "METRICS.md").write_text(
            """# Gold evidence bundle\n\nRelative trace days, not calendar dates. Capacity uses unique inventory server-hours. Request sums are observed evidence with incomplete lifecycle/demand semantics. Missing observations are unknown. Ratios use summed requests and matching inventory capacity, not fleet GPU utilization. SM remains in source units, not a0–100percentage. Do not average means/percentiles or sum distinct counts. Inspect planning_quality, source anomalies and lineage before interpretation. Execution is global; network spans only109–115. No AWS access is required to read these Parquet files.\n"""
        )
        for path in stage.iterdir():
            metadata["files"][path.name] = {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        metadata["total_bytes"] = sum(x["bytes"] for x in metadata["files"].values())
        (stage / "bundle.json").write_text(json.dumps(metadata, indent=2))
        for name, value in metadata["files"].items():
            if hashlib.sha256((stage / name).read_bytes()).hexdigest() != value["sha256"]:
                raise ValueError("Bundle hash check failed")
        stage.rename(destination)
        if store.remote:
            for path in destination.iterdir():
                store.s3.upload_file(
                    str(path), store.bucket, store.key(f"gold_exports/{release}/{path.name}")
                )
            store.write(f"manifests/gold/{release}/export.json", metadata)
        return metadata
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--release", required=True)
    p.add_argument("--destination", required=True)
    a = p.parse_args()
    print(json.dumps(export(Store(a.root), a.release, a.destination), indent=2))
