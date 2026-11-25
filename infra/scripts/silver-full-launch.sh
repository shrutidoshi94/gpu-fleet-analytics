set -euo pipefail
# The full run is now explicitly authorized; replace the short boot guard.
mkdir -p /etc/systemd/system/gpu-fleet-stop.timer.d
cat > /etc/systemd/system/gpu-fleet-stop.timer.d/full-run.conf <<'TIMER'
[Timer]
OnBootSec=
OnBootSec=34h
TIMER
systemctl stop gpu-fleet-stop.timer
cat > /opt/gpu-fleet/run-full-silver.sh <<'RUN'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
if [ -f logs/full-silver-20261119/supervisor.log ]; then
 cp logs/full-silver-20261119/supervisor.log logs/full-silver-20261119/supervisor.log.before-recovery
fi
exec python ops/silver_supervisor.py --run-id silver_full_20261119_r2 > logs/full-silver-20261119/supervisor.log 2>&1
RUN
cat > /opt/gpu-fleet/finalize-full-silver.sh <<'FINAL'
set +e
export AWS_DEFAULT_REGION=us-east-2
folder=/opt/gpu-fleet/logs/full-silver-20261119
printf '{"service_result":"%s","exit_code":"%s","exit_status":"%s"}\n' "${SERVICE_RESULT:-unknown}" "${EXIT_CODE:-unknown}" "${EXIT_STATUS:-unknown}" > "$folder/service-result.json"
# Independent watchdog survives this service's own cleanup timeout.
systemd-run --unit=gpu-silver-cleanup-stop --on-active=180s --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
python3 - <<'TERMINAL_STATUS'
import json,os
from datetime import datetime,timezone
from pathlib import Path
p=Path('/opt/gpu-fleet/logs/full-silver-20261119/status.json')
try:value=json.loads(p.read_text())
except Exception:value={}
result=os.environ.get('SERVICE_RESULT','unknown')
value.update(updated_utc=datetime.now(timezone.utc).isoformat(),supervisor_service_result=result,worker_shutdown_requested=True,
             run_outcome='success' if value.get('verified_complete') and result=='success' else ('failed' if result!='success' else 'interrupted'))
p.write_text(json.dumps(value,indent=2))
TERMINAL_STATUS
trap '/usr/sbin/shutdown -h now' EXIT
systemctl stop --no-block gpu-airflow-scheduler gpu-airflow-dag-processor gpu-airflow-api
sleep 3
timeout 30 runuser -u postgres -- pg_dump -Fc gpu_airflow > "$folder/airflow-metadata-final.dump"
journalctl -u gpu-silver-full --no-pager > "$folder/systemd.log"
df -h > "$folder/disk-final.txt"
timeout 45 aws s3 sync /opt/gpu-fleet/logs/airflow-production s3://__BUCKET__/run-logs/full-silver-20261119/airflow/ --only-show-errors
timeout 60 aws s3 sync "$folder" s3://__BUCKET__/run-logs/full-silver-20261119/ --only-show-errors
sync_result=$?
/usr/sbin/shutdown -h now
exit "$sync_result"
FINAL
cat > /etc/systemd/system/gpu-silver-full.service <<'UNIT'
[Unit]
Description=Authorized full GPU trace Bronze-to-Silver run
After=gpu-airflow-api.service gpu-airflow-scheduler.service gpu-airflow-dag-processor.service
[Service]
Type=exec
User=ec2-user
WorkingDirectory=/opt/gpu-fleet
ExecStart=/bin/bash /opt/gpu-fleet/run-full-silver.sh
ExecStopPost=+/bin/bash /opt/gpu-fleet/finalize-full-silver.sh
RuntimeMaxSec=31h
TimeoutStopSec=180
KillMode=control-group
UNIT
systemctl daemon-reload
systemctl start gpu-fleet-stop.timer
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python - <<'PY'
import json,subprocess
from sqlalchemy import select
from airflow import settings
from airflow.models.dagrun import DagRun
run_id='silver_full_20261119_r2'
with settings.Session() as session:
 smoke=session.scalar(select(DagRun.state).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id=='silver_reader_recovery_20261119'))
 exists=session.scalar(select(DagRun.run_id).where(DagRun.dag_id=='gpu_silver_backfill',DagRun.run_id==run_id))
assert smoke=='success',f'Production smoke not successful: {smoke}'
assert not exists,'Full run already exists; inspect it instead of duplicating'
subprocess.run(['airflow','dags','trigger','gpu_silver_backfill','--run-id',run_id,'--conf',json.dumps({'datasets':['server','pod','network','execution'],'days':list(range(185))})],check=True)
PY
USER
systemctl start gpu-silver-full.service
systemctl stop gpu-recovery-stop.timer || true
systemctl show gpu-silver-full -p ActiveState -p RuntimeMaxUSec -p MainPID
systemctl show gpu-fleet-stop.timer -p ActiveState -p NextElapseUSecMonotonic
