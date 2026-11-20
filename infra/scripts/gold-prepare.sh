set -euo pipefail
cd /opt/gpu-fleet
# Deploy only new Gold code. Do not replace the active Silver code or environment.
aws s3 cp s3://__BUCKET__/source/gold-20261120.tar.gz /opt/gpu-fleet/gold-20261120.tar.gz --only-show-errors
tar -xzf /opt/gpu-fleet/gold-20261120.tar.gz -C /opt/gpu-fleet
cp dags/gpu_gold.py production-dags/
mkdir -p logs/gold/gold_20261120
chown -R ec2-user:ec2-user gold.py ops/gold_task.py ops/gold_supervisor.py ops/export_gold.py production-dags/gpu_gold.py logs/gold
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python - <<'PY'
import importlib.util,json
from pathlib import Path
for path in ('gold.py','ops/gold_task.py','ops/gold_supervisor.py','ops/export_gold.py','production-dags/gpu_gold.py'):
 compile(Path(path).read_text(),path,'exec')
spec=importlib.util.spec_from_file_location('gold_dag_validation','production-dags/gpu_gold.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
assert module.dag.dag_id=='gpu_gold_backfill'
print('Gold DAG imports; task graph:',sorted(module.dag.task_ids))
PY
USER
cat > /opt/gpu-fleet/run-gold-full.sh <<'RUN'
set -euo pipefail
# Independent shutdown guard exists before canceling the short handoff guard.
systemd-run --unit=gpu-gold-runtime-stop --on-active=16h --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
# After= orders startup, not a predecessor's ExecStopPost completion.
# Explicitly wait for the Silver service to finish its checkpoint/finalizer.
for check in $(seq 1 120); do
 state=$(systemctl show gpu-silver-full.service -p ActiveState --value)
 case "$state" in inactive|failed) break ;; esac
 sleep 1
done
state=$(systemctl show gpu-silver-full.service -p ActiveState --value)
case "$state" in inactive|failed) ;; *) echo 'Silver finalizer did not finish'; exit 1 ;; esac
systemctl stop gpu-gold-handoff-stop.timer || true
systemctl start gpu-airflow-api gpu-airflow-scheduler gpu-airflow-dag-processor
exec runuser -u ec2-user -- bash -c 'cd /opt/gpu-fleet; source production.env.sh; export TRACE_GOLD_RELEASE=gold_20261120 TRACE_GOLD_LOG_DIR=/opt/gpu-fleet/logs/gold/gold_20261120; exec python ops/gold_supervisor.py' > /opt/gpu-fleet/logs/gold/gold_20261120/supervisor.log 2>&1
RUN
cat > /opt/gpu-fleet/finalize-gold.sh <<'FINAL'
set +e
systemd-run --unit=gpu-gold-cleanup-stop --on-active=180s --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
trap '/usr/sbin/shutdown -h now' EXIT
printf '{"service_result":"%s","exit_status":"%s"}\n' "${SERVICE_RESULT:-unknown}" "${EXIT_STATUS:-unknown}" > /opt/gpu-fleet/logs/gold/gold_20261120/service-result.json
systemctl stop --no-block gpu-airflow-scheduler gpu-airflow-dag-processor gpu-airflow-api
timeout 60 aws s3 sync /opt/gpu-fleet/logs/gold/gold_20261120 s3://__BUCKET__/run-logs/gold/gold_20261120/ --only-show-errors
/usr/sbin/shutdown -h now
FINAL
cat > /etc/systemd/system/gpu-gold-full.service <<'UNIT'
[Unit]
Description=Authorized Gold pilot and full release after verified Silver
After=gpu-silver-full.service network-online.target
[Service]
Type=exec
WorkingDirectory=/opt/gpu-fleet
ExecStart=/bin/bash /opt/gpu-fleet/run-gold-full.sh
ExecStopPost=/bin/bash /opt/gpu-fleet/finalize-gold.sh
RuntimeMaxSec=16h
TimeoutStopSec=180
KillMode=control-group
UNIT
systemctl daemon-reload
# Validate the exact finalizer -> After= successor ordering with harmless files.
probe=$(date +%s)
probe_dir=$(mktemp -d /tmp/gold-order.XXXXXX)
cat > "$probe_dir/finalize.sh" <<PROBE
systemctl start --no-block gpu-gold-order-b-$probe.service
sleep 2
touch "$probe_dir/predecessor-finalized"
PROBE
cat > "$probe_dir/wait.sh" <<PROBE
for check in \$(seq 1 20); do
 state=\$(systemctl show gpu-gold-order-a-$probe.service -p ActiveState --value)
 case "\$state" in inactive|failed) break ;; esac
 sleep 1
done
test -f "$probe_dir/predecessor-finalized"
PROBE
cat > "/etc/systemd/system/gpu-gold-order-b-$probe.service" <<PROBE
[Unit]
After=gpu-gold-order-a-$probe.service
[Service]
Type=oneshot
ExecStart=/bin/bash $probe_dir/wait.sh
PROBE
systemctl daemon-reload
systemd-run --unit=gpu-gold-order-a-$probe --property="ExecStopPost=/bin/bash $probe_dir/finalize.sh" /bin/true
sleep 5
test "$(systemctl show gpu-gold-order-b-$probe.service -p Result --value)" = success
test -f "$probe_dir/predecessor-finalized"
echo 'Finalizer-to-Gold service ordering probe passed'
# Arm handoff only after all files and DAG import validated.
python3 - <<'PY'
from pathlib import Path
p=Path('/opt/gpu-fleet/finalize-full-silver.sh')
s=p.read_text()
if 'GOLD_HANDOFF_20261120' not in s:
 p.with_suffix('.before-gold-handoff.sh').write_text(s)
 prefix='''# GOLD_HANDOFF_20261120: user authorized Gold after verified Silver, November20.
if [ "${SERVICE_RESULT:-unknown}" = success ] && python3 - <<'VERIFY_GOLD_PREREQUISITE'
import json
from pathlib import Path
s=json.loads(Path('/opt/gpu-fleet/logs/full-silver-20261119/status.json').read_text())
assert s.get('verified_complete') and s.get('dag_state')=='success'
assert Path('/opt/gpu-fleet/gold-ready-20261120').exists()
VERIFY_GOLD_PREREQUISITE
then
 systemd-run --unit=gpu-gold-handoff-stop --on-active=5m --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now
 if systemctl start --no-block gpu-gold-full.service; then
  python3 - <<'HANDOFF_STATUS'
import json
from pathlib import Path
p=Path('/opt/gpu-fleet/logs/full-silver-20261119/status.json')
s=json.loads(p.read_text());s.update(run_outcome='success',worker_shutdown_requested=False,handoff_gold_release='gold_20261120')
p.write_text(json.dumps(s,indent=2))
Path('/opt/gpu-fleet/logs/full-silver-20261119/service-result.json').write_text(json.dumps({'service_result':'success','handoff_gold_release':'gold_20261120'}))
HANDOFF_STATUS
  timeout 45 aws s3 sync /opt/gpu-fleet/logs/full-silver-20261119 s3://__BUCKET__/run-logs/full-silver-20261119/ --only-show-errors
  exit 0
 fi
fi
'''
 s=s.replace('set +e\n','set +e\n'+prefix,1)
 p.write_text(s)
PY
bash -n /opt/gpu-fleet/finalize-full-silver.sh
bash -n /opt/gpu-fleet/run-gold-full.sh
bash -n /opt/gpu-fleet/finalize-gold.sh
touch /opt/gpu-fleet/gold-ready-20261120
aws s3 cp - s3://__BUCKET__/run-logs/gold/gold_20261120/queued.json --only-show-errors <<'JSON'
{"release":"gold_20261120","state":"waiting_for_verified_silver","pilot_days":[3,5,109,173],"full_days":185,"pilot_projection_budget_hours":12,"runtime_guard_hours":16,"same_worker":true,"authorization":"user requested commit to GitHub and start Gold transformations"}
JSON
systemctl is-active gpu-silver-full
printf 'Gold handoff armed; Silver remains active. Gold has not started.\n'
