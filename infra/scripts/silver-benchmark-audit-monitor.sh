set -euo pipefail
cat > /opt/gpu-fleet/monitor-silver-audits.py <<'PY'
import json,subprocess,time
from pathlib import Path
root=Path('/opt/gpu-fleet/logs/silver-benchmark-20261119')
seen={}
while True:
    snapshot=subprocess.check_output(['ps','-eo','pid=,etimes=,args='],text=True)
    present=set()
    for line in snapshot.splitlines():
        parts=line.split(None,2)
        if len(parts)<3 or '--audit-only' not in parts[2] or 'silver.py' not in parts[2]: continue
        pid,elapsed,args=int(parts[0]),int(parts[1]),parts[2]
        present.add(pid)
        row=seen.setdefault(pid,{'command':args,'first_seen_utc_epoch':time.time(),'elapsed_when_first_seen':elapsed})
        row.update(elapsed_last_seen=elapsed,last_seen_utc_epoch=time.time())
    for pid,row in seen.items():
        if pid not in present and 'finished_observed_utc_epoch' not in row:
            row['finished_observed_utc_epoch']=time.time()
            row['estimated_wall_seconds']=row['elapsed_last_seen']+1
    (root/'audit-process-timings.json').write_text(json.dumps(list(seen.values()),indent=2))
    state=subprocess.check_output(['systemctl','show','gpu-silver-benchmark.service','-p','ActiveState','--value'],text=True).strip()
    if state not in ('active','activating'): break
    time.sleep(1)
PY
systemd-run --unit=gpu-silver-audit-monitor --property=RuntimeMaxSec=100m /opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/monitor-silver-audits.py
