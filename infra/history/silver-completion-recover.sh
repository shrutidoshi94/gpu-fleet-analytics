set -euo pipefail
cd /opt/gpu-fleet
systemd-run --unit=gpu-completion-recovery-stop --on-active=30m --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
folder=/opt/gpu-fleet/logs/full-silver-20261119
cp "$folder/status.json" "$folder/completion-import-failure.json"
cp "$folder/supervisor.log" "$folder/supervisor.before-completion-import-fix.log"
aws s3 cp "$folder/completion-import-failure.json" s3://__BUCKET__/run-logs/full-silver-20261119/completion-import-failure.json --only-show-errors
aws s3 cp s3://__BUCKET__/source/silver-supervisor-import-fix.py ops/silver_supervisor.py --only-show-errors
chown ec2-user:ec2-user ops/silver_supervisor.py
# The short recovery timer is replaced only once Gold's independent16h timer exists.
python3 - <<'PY'
from pathlib import Path
p=Path('/opt/gpu-fleet/run-gold-full.sh');s=p.read_text()
line='systemctl stop gpu-completion-recovery-stop.timer || true\n'
if line not in s:s=s.replace('systemctl stop gpu-gold-handoff-stop.timer || true\n','systemctl stop gpu-gold-handoff-stop.timer || true\n'+line)
p.write_text(s)
PY
bash -n run-gold-full.sh
systemctl start postgresql gpu-airflow-api gpu-airflow-scheduler gpu-airflow-dag-processor
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python -I - <<'PY'
import runpy,importlib
runpy.run_path('/opt/gpu-fleet/ops/silver_supervisor.py',run_name='probe')
m=importlib.import_module('silver')
print('Core import works:',m.CODE_SHA256)
from airflow import settings
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from sqlalchemy import select,func
with settings.Session() as s:
 state=s.scalar(select(DagRun.state).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id=='silver_full_20261119_r2'))
 count=s.scalar(select(func.count()).select_from(TaskInstance).where(TaskInstance.dag_id=='gpu_silver_backfill',TaskInstance.run_id=='silver_full_20261119_r2',TaskInstance.state=='success'))
assert state=='success' and count==380,(state,count)
print('Existing Airflow run complete:380 successful tasks. No Spark rerun.')
PY
USER
systemctl start gpu-fleet-stop.timer
systemctl reset-failed gpu-silver-full || true
systemctl start gpu-silver-full
systemctl show gpu-silver-full gpu-gold-full -p Id -p ActiveState -p MainPID
