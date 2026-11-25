set -euo pipefail
systemctl start postgresql
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python - <<'PY'
import json
from pathlib import Path
from datetime import datetime,timezone
from sqlalchemy import select
from airflow import settings
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
r=json.loads(Path('logs/full-silver-20261119/whole-bronze-preflight.json').read_text())
assert r['status']=='passed' and not r['errors']
with settings.Session() as s:
 old=s.scalar(select(DagRun).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id=='silver_full_20261119'))
 changes=[]
 if old:
  old.state='failed'
  tasks=s.scalars(select(TaskInstance).where(TaskInstance.dag_id=='gpu_silver_backfill',TaskInstance.run_id==old.run_id)).all()
  for task in tasks:
   if task.state not in ('success','failed','skipped','upstream_failed'):
    changes.append({'task':task.task_id,'map_index':task.map_index,'old_state':task.state})
    task.state='failed' if task.state=='running' else 'upstream_failed'
    task.end_date=datetime.now(timezone.utc)
  s.commit()
 Path('logs/full-silver-20261119/recovery-state-changes.json').write_text(json.dumps({'reason':'permanent failure, workers stopped; preserve old run and commits','changes':changes},indent=2))
PY
USER
systemctl reset-failed gpu-silver-full.service || true
systemctl start gpu-airflow-api gpu-airflow-dag-processor gpu-airflow-scheduler
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python - <<'PY'
import json,subprocess,time
from sqlalchemy import select
from airflow import settings
from airflow.models.dagrun import DagRun
run_id='silver_reader_recovery_20261119'
subprocess.run(['airflow','dags','trigger','gpu_silver_backfill','--run-id',run_id,'--conf',json.dumps({'datasets':['server','pod'],'days':[5]})],check=True)
for _ in range(240):
 with settings.Session() as s:
  state=s.scalar(select(DagRun.state).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id==run_id))
 if state=='success':
  from silver import Store,CODE_SHA256
  store=Store('s3://__BUCKET__');commit=store.read('manifests/silver/pod/day=5.current.json')
  assert commit and commit['code_sha256']==CODE_SHA256
  assert commit['quality']['source_observations'],'Anomalous-hour warning missing'
  print('Recovery day5 smoke SUCCEEDED; rows:',commit['quality']['input_rows'],'source observations:',len(commit['quality']['source_observations']))
  break
 if state=='failed':raise RuntimeError('Recovery smoke failed')
 time.sleep(2)
else:raise TimeoutError('Recovery smoke did not complete in8minutes')
PY
USER
