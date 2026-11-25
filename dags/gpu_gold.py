"""Manual, serial Gold release: validated Silver -> pilot -> daily reductions -> export."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
from airflow.sdk import DAG, Param, task

with DAG('gpu_gold_backfill',schedule=None,catchup=False,
         start_date=datetime(2026,1,1,tzinfo=timezone.utc),max_active_runs=1,max_active_tasks=1,
         params={'release':Param('gold_20261120',type='string',pattern='^[A-Za-z0-9_-]+$'),
                 'days':Param(list(range(185)),type='array',minItems=1,uniqueItems=True,
                              items={'type':'integer','minimum':0,'maximum':184})}) as dag:
    def command(action, release, extra=()):
        project=Path(os.environ['TRACE_PROJECT'])
        subprocess.run([sys.executable,str(project/'ops/gold_task.py'),action,'--release',release,
                        '--root',os.environ['TRACE_SILVER_ROOT'],*extra],cwd=project,check=True)

    @task(execution_timeout=timedelta(minutes=30))
    def pin_release(params):
        command('pin',params['release'],['--days',','.join(map(str,params['days']))])
        return params['release']

    @task(retries=0,execution_timeout=timedelta(hours=2))
    def pilot(release,params):
        for day in [d for d in (3,5,109,173) if d in params['days']]:
            command('day',release,['--day',str(day)])
        command('pilot-check',release)

    @task
    def remaining(release,params):
        return [{'release':release,'day':d} for d in sorted(params['days']) if d not in (3,5,109,173)]

    @task(retries=1,execution_timeout=timedelta(minutes=75))
    def gold_day(release,day):
        command('day',release,['--day',str(day)])

    @task(retries=1,execution_timeout=timedelta(hours=1))
    def global_context(release):
        command('context',release)

    @task(execution_timeout=timedelta(hours=1))
    def validate_publish_export(release):
        command('publish',release)
        command('export',release)

    release=pin_release()
    gate=pilot(release)
    work=remaining(release)
    gate >> work
    reduced=gold_day.expand_kwargs(work)
    context=global_context(release)
    reduced >> context >> validate_publish_export(release)
