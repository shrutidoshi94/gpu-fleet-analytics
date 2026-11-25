set -euo pipefail
cat > /opt/gpu-fleet/monitor-silver-host.py <<'PY'
import csv,shutil,subprocess,time
from pathlib import Path
root=Path('/opt/gpu-fleet/logs/silver-benchmark-20261119')
def sample():
    cpu=[int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
    line=next(x.split() for x in Path('/proc/diskstats').read_text().splitlines() if x.split()[2]=='nvme0n1')
    net=[x.split(':',1)[1].split() for x in Path('/proc/net/dev').read_text().splitlines()[2:] if x.split(':',1)[0].strip()!='lo']
    return time.time(),sum(cpu),cpu[3]+cpu[4],int(line[5])*512,int(line[9])*512,int(line[12]),sum(int(x[0]) for x in net),sum(int(x[8]) for x in net)
metrics=root/'host-metrics.csv'
if metrics.exists():metrics.rename(metrics.with_name(metrics.name+f'.prior-{int(time.time())}'))
with metrics.open('w',buffering=1) as stream:
    writer=csv.writer(stream);writer.writerow(['utc_epoch','cpu_busy_percent','disk_read_mib_s','disk_write_mib_s','disk_busy_percent','network_rx_mib_s','network_tx_mib_s','free_disk_gib','available_memory_gib'])
    before=sample()
    while True:
        time.sleep(2); now=sample();dt=now[0]-before[0]
        memory={line.split(':')[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
        writer.writerow([now[0],100*(1-(now[2]-before[2])/max(1,now[1]-before[1])),(now[3]-before[3])/dt/2**20,(now[4]-before[4])/dt/2**20,(now[5]-before[5])/dt/10,(now[6]-before[6])/dt/2**20,(now[7]-before[7])/dt/2**20,shutil.disk_usage(root).free/2**30,memory['MemAvailable']/2**20])
        before=now
        state=subprocess.check_output(['systemctl','show','gpu-silver-benchmark.service','-p','ActiveState','--value'],text=True).strip()
        if state not in ('active','activating'):break
PY
systemd-run --unit=gpu-silver-host-monitor --property=RuntimeMaxSec=100m /opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/monitor-silver-host.py
