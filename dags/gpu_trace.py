import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, task

with DAG("gpu_trace_day", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
         catchup=False, max_active_runs=1,
         params={"from_bronze": Param(False, type="boolean"), "dataset": Param("network", enum=["network", "pod", "server", "execution"]),
                 "day": Param(109, type="integer", minimum=0, maximum=184)}) as dag:
    @task(retries=1)
    def process_day(params):
        project = Path(os.environ["TRACE_PROJECT"])
        command = [sys.executable, str(project / "pipelines/pipeline.py"), params["dataset"], str(params["day"])]
        if params["from_bronze"]:
            command.append("--from-bronze")
        subprocess.run(command, cwd=project, check=True)

    process_day()
