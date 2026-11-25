"""Run-scoped Gold controller: Airflow trigger, health checks and shutdown outcome."""

import json
import os
import shutil
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipelines.silver import Store


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    from airflow import settings
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from sqlalchemy import select

    release = os.environ["TRACE_GOLD_RELEASE"]
    run_id = release
    store = Store(os.environ["TRACE_SILVER_ROOT"])
    folder = Path(os.environ["TRACE_GOLD_LOG_DIR"])
    folder.mkdir(parents=True, exist_ok=True)
    silver = store.read(
        os.environ.get("TRACE_SILVER_STATUS_KEY", "run-logs/full-silver-20261119/status.json")
    )
    if not silver or not silver.get("verified_complete") or silver.get("dag_state") != "success":
        raise RuntimeError("Gold requires verified complete Silver")
    # Preserve final Silver evidence before the Gold run uses the same worker.
    store.write(f"run-logs/gold/{release}/silver-prerequisite.json", silver)
    started = time.monotonic()
    while True:
        with settings.Session() as session:
            exists = session.scalar(
                select(DagModel.dag_id).where(DagModel.dag_id == "gpu_gold_backfill")
            )
        if exists:
            break
        if time.monotonic() - started > 180:
            raise RuntimeError("Gold DAG did not register")
        time.sleep(5)
    subprocess.run(["airflow", "dags", "unpause", "gpu_gold_backfill"], check=True)
    with settings.Session() as session:
        previous = session.scalar(
            select(DagRun.state).where(
                DagRun.dag_id == "gpu_gold_backfill", DagRun.run_id == run_id
            )
        )
    if previous is not None:
        raise RuntimeError("Run already exists; inspect before restarting")
    subprocess.run(
        [
            "airflow",
            "dags",
            "trigger",
            "gpu_gold_backfill",
            "--run-id",
            run_id,
            "--conf",
            json.dumps({"release": release, "days": list(range(185))}),
        ],
        check=True,
    )
    started = time.monotonic()
    last_backup = started - 7200
    while True:
        with settings.Session() as session:
            state = session.scalar(
                select(DagRun.state).where(
                    DagRun.dag_id == "gpu_gold_backfill", DagRun.run_id == run_id
                )
            )
            tasks = list(
                session.execute(
                    select(TaskInstance.task_id, TaskInstance.map_index, TaskInstance.state).where(
                        TaskInstance.dag_id == "gpu_gold_backfill", TaskInstance.run_id == run_id
                    )
                )
            )
        counts = Counter(t.state or "not_scheduled" for t in tasks)
        services = {
            n: subprocess.check_output(
                ["systemctl", "show", "gpu-airflow-" + n, "-p", "ActiveState", "--value"], text=True
            ).strip()
            for n in ("api", "scheduler", "dag-processor")
        }
        value = {
            "updated_utc": now(),
            "release": release,
            "run_id": run_id,
            "dag_state": state,
            "task_states": dict(counts),
            "runtime_hours": (time.monotonic() - started) / 3600,
            "free_disk_gib": shutil.disk_usage(folder).free / 2**30,
            "services": services,
            "active_tasks": [
                {"task": t.task_id, "map_index": t.map_index, "state": t.state}
                for t in tasks
                if t.state in ("running", "queued", "up_for_retry")
            ],
            "failed_tasks": [
                {"task": t.task_id, "map_index": t.map_index, "state": t.state}
                for t in tasks
                if t.state in ("failed", "upstream_failed")
            ],
        }
        if time.monotonic() - last_backup >= 7200:
            # PG dump runs as local ec2-user with credentials inherited privately from production.env.sh.
            from urllib.parse import urlparse

            uri = urlparse(
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace(
                    "postgresql+psycopg2", "postgresql"
                )
            )
            env = os.environ.copy()
            env["PGPASSWORD"] = uri.password
            subprocess.run(
                [
                    "pg_dump",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    uri.username,
                    "-d",
                    uri.path.strip("/"),
                    "-Fc",
                    "-f",
                    str(folder / "metadata.dump"),
                ],
                env=env,
                check=True,
                timeout=60,
            )
            store.s3.upload_file(
                str(folder / "metadata.dump"),
                store.bucket,
                f"run-logs/gold/{release}/metadata.dump",
            )
            last_backup = time.monotonic()
        (folder / "status.json").write_text(json.dumps(value, indent=2))
        store.write(f"run-logs/gold/{release}/status.json", value)
        print(json.dumps(value), flush=True)
        if state == "failed" or value["failed_tasks"]:
            raise RuntimeError("Gold Airflow task failed")
        if any(v != "active" for v in services.values()):
            raise RuntimeError("Gold Airflow service down")
        if value["free_disk_gib"] < 10:
            raise RuntimeError("Gold global disk guard")
        if time.monotonic() - started > 15 * 3600:
            raise RuntimeError("Gold15-hour supervisor limit")
        if state == "success":
            report = store.read(f"manifests/gold/{release}/release.json")
            export = store.read(f"manifests/gold/{release}/export.json")
            if (
                not report
                or not export
                or len(report["units"]) != 186
                or export["plan_identity"] != report["plan_identity"]
            ):
                raise RuntimeError("Gold release/export incomplete")
            value.update(
                verified_complete=True,
                gold_day_partitions=185,
                bundle_bytes=export["total_bytes"],
                completed_utc=now(),
            )
            (folder / "status.json").write_text(json.dumps(value, indent=2))
            store.write(f"run-logs/gold/{release}/status.json", value)
            return
        time.sleep(60)


if __name__ == "__main__":
    main()
