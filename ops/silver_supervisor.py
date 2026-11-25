"""Run-scoped Airflow health monitoring, checkpoints and completion verification."""
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[1] / 'pipelines'))
import argparse
from collections import Counter
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def now():return datetime.now(timezone.utc).isoformat()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',required=True)
    args=parser.parse_args()
    from airflow import settings
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    import boto3
    from sqlalchemy import select
    root=Path(os.environ['TRACE_RUN_LOG_DIR']);root.mkdir(parents=True,exist_ok=True)
    client=boto3.client('s3');bucket=os.environ['TRACE_BUCKET']
    started=time.monotonic();started_utc=now()
    previous_status=root/'status.json'
    if previous_status.exists():
        previous=json.loads(previous_status.read_text())
        if previous.get('run_id')==args.run_id and previous.get('started_utc'):
            started_utc=previous['started_utc']
    last_check=time.monotonic()-7200;last_backup=0;last_count=-1;last_progress=time.time()
    def publish(value):
        (root/'status.json').write_text(json.dumps(value,indent=2))
        client.put_object(Bucket=bucket,Key='run-logs/full-silver-20261119/status.json',Body=json.dumps(value).encode(),ContentType='application/json')
    while True:
        with settings.Session() as session:
            run=session.scalar(select(DagRun).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id==args.run_id))
            if run is None:raise RuntimeError('Authorized Airflow run not found')
            tasks=list(session.execute(select(TaskInstance.task_id,TaskInstance.map_index,TaskInstance.state).where(TaskInstance.dag_id=='gpu_silver_backfill',TaskInstance.run_id==args.run_id)))
            state=run.state
        counts=Counter(str(t.state or 'not_scheduled') for t in tasks)
        finished=counts['success']+counts['skipped']
        if finished!=last_count:last_progress=time.time();last_count=finished
        units={name:subprocess.check_output(['systemctl','show',f'gpu-airflow-{name}','--property=ActiveState','--value'],text=True).strip() for name in ('api','scheduler','dag-processor')}
        value={'started_utc':started_utc,'updated_utc':now(),'run_id':args.run_id,'dag_state':state,'task_states':dict(counts),
               'runtime_hours':(datetime.now(timezone.utc)-datetime.fromisoformat(started_utc)).total_seconds()/3600,'seconds_since_task_progress':time.time()-last_progress,
               'free_disk_gib':shutil.disk_usage(root).free/2**30,'services':units,'expected_transform_partitions':378,
               'active_tasks':[{'task':t.task_id,'map_index':t.map_index,'state':t.state} for t in tasks if t.state in ('running','queued','up_for_retry')],
               'failed_tasks':[{'task':t.task_id,'map_index':t.map_index,'state':t.state} for t in tasks if t.state in ('failed','upstream_failed')]}
        # Every two hours, persist a comprehensive check and local metadata backup.
        if time.monotonic()-last_check>=7200:
            last_check=time.monotonic()
            records=[]
            for p in root.glob('*.json'):
                if p.name.startswith(('transform-','audit-')):
                    row=json.loads(p.read_text())
                    if row.get('status')=='success':records.append(row)
            value['health_check']={'completed_task_reports':len(records),'row_accounting_ok':all(x['quality']['input_rows']==x['quality']['accepted_rows']+x['quality']['quarantined_rows'] for x in records),
                                   'quarantined_rows_across_reports':sum(x['quality']['quarantined_rows'] for x in records),
                                   'unmatched_final_audit_observations':sum(x.get('final_audit',{}).get('unmatched_observations',0) for x in records)}
            with (root/'health-checks.jsonl').open('a') as stream:stream.write(json.dumps(value)+'\n')
            uri=os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']
            from urllib.parse import urlparse
            parsed=urlparse(uri.replace('postgresql+psycopg2','postgresql'))
            env=os.environ.copy();env['PGPASSWORD']=parsed.password
            subprocess.run(['pg_dump','-h','127.0.0.1','-U',parsed.username,'-d',parsed.path.strip('/'),'-Fc','-f',str(root/'airflow-metadata.dump')],env=env,check=True)
            last_backup=time.monotonic()
            client.upload_file(str(root/'airflow-metadata.dump'),bucket,'run-logs/full-silver-20261119/airflow-metadata.dump')
            client.upload_file(str(root/'health-checks.jsonl'),bucket,'run-logs/full-silver-20261119/health-checks.jsonl')
        publish(value)
        print(json.dumps({k:value[k] for k in ['updated_utc','dag_state','task_states','free_disk_gib']}),flush=True)
        if value['failed_tasks'] or state=='failed':raise RuntimeError('Airflow permanent failure; stopping compute after checkpointing')
        if any(s!='active' for s in units.values()):raise RuntimeError('Required Airflow service is down')
        if value['free_disk_gib']<10:raise RuntimeError('Global scratch-space guard')
        if state=='success':
            from silver import Store,CODE_SHA256,partition,source_inventory
            store=Store('s3://'+bucket)
            total=0;accepted=0;quarantined=0;inventory_by_day={}
            for ds in ('server','pod','network','execution'):
                days=range(109,116) if ds=='network' else ([0] if ds=='execution' else range(185))
                for day in days:
                    part=partition(ds,day);commit=store.read(f'manifests/silver/{ds}/{part}.current.json')
                    if not commit or commit['code_sha256']!=CODE_SHA256:raise RuntimeError(f'Missing/current-code commit: {ds}/{part}')
                    if commit['source_fingerprint']!=source_inventory(store,ds,day)[2]:raise RuntimeError(f'Source changed: {ds}/{part}')
                    if any(store.files(commit[area])!=commit['output_files'][area] for area in ('accepted','quarantine')):
                        raise RuntimeError(f'Output inventory changed: {ds}/{part}')
                    if ds=='server':inventory_by_day[day]=commit['source_fingerprint']
                    q=commit['quality']
                    if q['input_rows']!=q['accepted_rows']+q['quarantined_rows']:raise RuntimeError('Final row accounting mismatch')
                    if ds in ('pod','network'):
                        audit=store.read(f'manifests/silver_audits/{ds}/{part}.json')
                        if not audit or audit['silver_attempt']!=commit['accepted'] or audit['inventory_source_fingerprint']!=inventory_by_day[day]:raise RuntimeError('Missing/current-attempt final audit')
                    total+=1;accepted+=q['accepted_rows'];quarantined+=q['quarantined_rows']
            assert total==378,total
            value.update(verified_complete=True,verified_partitions=total,accepted_rows=accepted,quarantined_rows=quarantined,completed_utc=now())
            publish(value)
            print('All378 Silver commits and final audits verified; stopping worker.',flush=True)
            return
        if value['runtime_hours']>30:raise TimeoutError('Full-run30-hour runtime guard')
        time.sleep(60)

if __name__=='__main__':main()
