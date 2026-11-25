set -euo pipefail
cat > /opt/gpu-fleet/publish-download-progress.py <<'PY'
import json,subprocess,time,shutil
from pathlib import Path
root=Path('/data/gpu-fleet')
sizes={'asi_opensource_network_hourly':203904100,'asi_opensource_server_hourly':3080121387,'asi_opensource_job_execution_summary':1188295031,'asi_opensource_pod_hourly':351803513445}
status=json.loads((root/'download-logs/status.json').read_text())
files={k:min((root/'archives'/f'{k}.zip').stat().st_size if (root/'archives'/f'{k}.zip').exists() else 0,v) for k,v in sizes.items()}
report={'timestamp':time.time(),'state':status['state'],'current':status.get('current'),'downloaded_bytes':sum(files.values()),'total_bytes':sum(sizes.values()),'percent':100*sum(files.values())/sum(sizes.values()),'files':files,'archive_states':{k:v['state'] for k,v in status['archives'].items()},'free_bytes':shutil.disk_usage(root).free,'error':status.get('error'),'service_state':subprocess.check_output(['systemctl','show','gpu-fleet-download','-p','ActiveState','--value'],text=True).strip()}
p=root/'download-logs/progress.json';p.write_text(json.dumps(report,indent=2))
subprocess.run(['aws','s3','cp',str(p),'s3://__BUCKET__/run-logs/downloads/progress.json','--region','us-east-2','--only-show-errors'],check=True)
if status['state']!='running':
    subprocess.run(['aws','s3','sync',str(root/'download-logs'),'s3://__BUCKET__/run-logs/downloads/','--region','us-east-2','--exclude','*.curl.log','--only-show-errors'],check=True)
PY
cat > /opt/gpu-fleet/report-download-progress.sh <<'REPORT'
#!/bin/bash
while true; do
  /opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/publish-download-progress.py
  sleep 60
done
REPORT
chmod 755 /opt/gpu-fleet/report-download-progress.sh
cat > /run/systemd/system/gpu-fleet-download.service.d/runtime.conf <<'EOF'
[Service]
RuntimeMaxSec=11h
ExecStopPost=
ExecStopPost=+/opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/publish-download-progress.py
ExecStopPost=+/usr/sbin/shutdown -h now
EOF
systemctl daemon-reload
systemd-run --unit=gpu-fleet-download-reporter /opt/gpu-fleet/report-download-progress.sh
/opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/publish-download-progress.py
cat /data/gpu-fleet/download-logs/progress.json
