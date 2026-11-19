import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, task

with DAG("gpu_trace_day", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
         catchup=False, max_active_runs=1,
         params={"dataset": Param("network", enum=["network", "pod"]),
                 "day": Param(109, type="integer", minimum=0, maximum=184)}) as dag:
    @task(retries=1)
    def process_day(params):
        project = Path(os.environ["TRACE_PROJECT"])
        subprocess.run([sys.executable, str(project / "pipeline.py"),
                        params["dataset"], str(params["day"])], cwd=project, check=True)

    process_day()
