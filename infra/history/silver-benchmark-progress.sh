set -u
systemctl show gpu-silver-benchmark.service -p ActiveState -p SubState -p Result
python3 - <<'PY'
import json,shutil
from pathlib import Path
root=Path('/opt/gpu-fleet/logs/silver-benchmark-20261119')
p=root/'results.json'
if p.exists():
 r=json.loads(p.read_text());print(json.dumps({'status':r['status'],'completed':[{'case':x['benchmark_case'],'seconds':round(x['wall_seconds'],1),'rss_gib':round(x['peak_process_group_rss_kib']/2**20,2),'disk_spill_gib':round(x['spark_task_metrics']['disk_spilled_bytes']/2**30,3),'minimum_free_gib':round(x['min_free_disk_bytes']/2**30,2),'rows':x['quality']['input_rows']} for x in r['reports']]}))
print('Free root GiB:',round(shutil.disk_usage('/opt/gpu-fleet').free/2**30,2))
if (root/'host-metrics.csv').exists():
 import csv
 rows=list(csv.DictReader((root/'host-metrics.csv').open()))[-15:]
 if rows:
  print('Recent host means:',{k:round(sum(float(row[k]) for row in rows)/len(rows),2) for k in ['cpu_busy_percent','disk_read_mib_s','disk_write_mib_s','network_rx_mib_s','available_memory_gib']})
for name in ['runner.log','airflow-smoke.log']:
 p=root/name
 if p.exists():
  with p.open(errors='replace') as f:
   from collections import deque
   print(name+':\n'+''.join(deque(f,maxlen=4)))
for p in sorted(root.glob('*threads.log')):
 print(p.name,p.stat().st_size)
 if p.stat().st_size:
  with p.open(errors='replace') as f:
   from collections import deque
   print(''.join(deque(f,maxlen=2)))

PY
