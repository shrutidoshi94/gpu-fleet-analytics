"""Serial daily backfill over immutable, verified S3 Bronze datasets."""
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from airflow.sdk import DAG, Param, task

with DAG('gpu_medallion_backfill', schedule=None,
         start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
         max_active_runs=1, max_active_tasks=1,
         params={'datasets': Param(['server', 'pod', 'network', 'execution'], type='array',
                                   items={'type': 'string', 'enum': ['server', 'pod', 'network', 'execution']},
                                   minItems=1, uniqueItems=True),
                 'start_day': Param(109, type='integer', minimum=0, maximum=184),
                 'end_day': Param(109, type='integer', minimum=0, maximum=184)}) as dag:
    @task
    def plan(params):
        start, end = params['start_day'], params['end_day']
        if end < start:
            raise ValueError('end_day must be at least start_day')
        datasets = params['datasets']
        items = [{'dataset': 'execution', 'day': 0}] if 'execution' in datasets else []
        for day in range(start, end + 1):
            for dataset in ['server', 'pod'] + (['network'] if 109 <= day <= 115 else []):
                if dataset in datasets:
                    items.append({'dataset': dataset, 'day': day})
        if not items:
            raise ValueError('No selected datasets cover the requested days')
        return items

    @task(retries=1, pool='default_pool')
    def process_day(dataset, day):
        project = Path(os.environ['TRACE_PROJECT'])
        subprocess.run([sys.executable, str(project / 'pipeline.py'), dataset, str(day),
                        '--from-bronze'], cwd=project, check=True)

    process_day.expand_kwargs(plan())
