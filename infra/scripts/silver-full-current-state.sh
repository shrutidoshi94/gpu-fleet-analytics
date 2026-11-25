set -u
systemctl show gpu-silver-full -p ActiveState -p SubState -p Result -p ExecMainStatus -p ExecStopPost
systemctl is-active gpu-airflow-api gpu-airflow-scheduler gpu-airflow-dag-processor || true
python3 - <<'PY'
from pathlib import Path
from collections import deque
root=Path('/opt/gpu-fleet/logs/full-silver-20261119')
for name in ['service-result.json','supervisor.log','disk-final.txt']:
 p=root/name
 if p.exists():print(name+':\n'+''.join(deque(p.open(errors='replace'),maxlen=25)))
import json
for p in root.glob('transform-pod-5-*.json'):
 r=json.loads(p.read_text());print(p.name,r.get('status'),r.get('error'),r.get('log'))
 log=root/r['log']
 if log.exists():
  lines=log.read_text(errors='replace').splitlines()
  for i,line in enumerate(lines):
   if any(x in line for x in ['Traceback','ValueError:','Error:','Exception:']):print('\n'.join(lines[max(0,i-1):i+9]))
PY
