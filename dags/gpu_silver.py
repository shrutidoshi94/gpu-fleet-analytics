"""Silver-only mapped tasks; serial writer, no Gold, no automatic schedule.
LocalExecutor runs on ONE machine. Multi-worker deployment is not configured.
"""
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import subprocess
import sys
from airflow.sdk import DAG, Param, task

with DAG('gpu_silver_backfill',schedule=None,catchup=False,
         start_date=datetime(2026,1,1,tzinfo=timezone.utc),max_active_runs=1,max_active_tasks=1,
         params={'datasets':Param(['server','pod','network'],type='array',minItems=1,uniqueItems=True,
                                 items={'type':'string','enum':['server','pod','network','execution']}),
                 'days':Param([109],type='array',minItems=1,uniqueItems=True,
                              items={'type':'integer','minimum':0,'maximum':184})}) as dag:
    @task
    def plan(params):
        items=[{'dataset':ds,'day':day} for day in sorted(params['days'])
               for ds in ('server','pod','network') if ds in params['datasets'] and (ds!='network' or 109<=day<=115)]
        if 'execution' in params['datasets']: items.append({'dataset':'execution','day':0})
        if not items: raise ValueError('No selected datasets cover requested days')
        return items
    @task(retries=1, execution_timeout=timedelta(minutes=75))
    def silver_partition(dataset,day):
        project=Path(os.environ['TRACE_PROJECT'])
        command=[sys.executable,str(project/'ops/silver_task.py'),dataset,str(day),'--root',os.environ['TRACE_SILVER_ROOT'],
                 '--master',os.environ.get('TRACE_SPARK_MASTER','local[2]'),
                 '--output-partitions',os.environ.get('TRACE_OUTPUT_PARTITIONS','4')]
        if os.environ.get('TRACE_SPARK_EVENT_LOG_DIR'): command+=['--event-log-dir',os.environ['TRACE_SPARK_EVENT_LOG_DIR']]
        subprocess.run(command,cwd=project,check=True)
    @task(retries=1, execution_timeout=timedelta(hours=5))
    def audit_committed_partitions(params):
        project=Path(os.environ['TRACE_PROJECT'])
        for day in sorted(params['days']):
            for dataset in ('pod','network'):
                if dataset not in params['datasets'] or (dataset=='network' and not 109<=day<=115):
                    continue
                subprocess.run([sys.executable,str(project/'ops/silver_task.py'),dataset,str(day),
                                '--root',os.environ['TRACE_SILVER_ROOT'],'--audit-only',
                                '--master',os.environ.get('TRACE_SPARK_MASTER','local[2]')],cwd=project,check=True)
    silver_partition.expand_kwargs(plan()) >> audit_committed_partitions()
