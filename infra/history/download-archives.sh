set -euo pipefail
mountpoint -q /data/gpu-fleet
[ "$(findmnt -n -o FSTYPE /data/gpu-fleet)" = xfs ]
mkdir -p /data/gpu-fleet/download-logs
chown ec2-user:ec2-user /data/gpu-fleet/download-logs
cat > /opt/gpu-fleet/download-archives.py <<'PY'
import hashlib,json,os,shutil,subprocess,time
from pathlib import Path
from zipfile import ZipFile
root=Path('/data/gpu-fleet')
archives=root/'archives'; logs=root/'download-logs'
items=[('asi_opensource_server_hourly',3080121387),('asi_opensource_job_execution_summary',1188295031),('asi_opensource_pod_hourly',351803513445)]
status={'state':'running','started_at':time.time(),'archives':{}}
def save():
    tmp=logs/'status.tmp'
    tmp.write_text(json.dumps(status,indent=2)); tmp.replace(logs/'status.json')
save()
try:
    for table,size in items:
        path=archives/(table+'.zip')
        existing=path.stat().st_size if path.exists() else 0
        assert existing<=size,(path,existing,size)
        assert shutil.disk_usage(root).free > (size-existing)+20*1024**3,'Insufficient free space with 20 GiB reserve'
        status['current']=table; status['archives'][table]={'expected_bytes':size,'state':'downloading'};save()
        if existing<size:
            with (logs/(table+'.curl.log')).open('a') as log:
                subprocess.run(['curl','--fail','--location','--continue-at','-','--retry','5','--retry-delay','10','--connect-timeout','30','--speed-limit','1024','--speed-time','180','--output',str(path),'https://tre-clusterdata.oss-cn-hangzhou.aliyuncs.com/cluster-trace-gpu-v2026/data/'+table+'.zip'],stdout=log,stderr=log,check=True)
        assert path.stat().st_size==size,(path,path.stat().st_size,size)
        status['archives'][table]['state']='verifying';save()
        digest=hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda:f.read(16*1024**2),b''): digest.update(chunk)
        with ZipFile(path) as z:
            members=z.infolist()
            days={}
            for member in members:
                for part in member.filename.split('/'):
                    if part.startswith('day='):
                        days[part]=days.get(part,0)+member.file_size
            evidence={'bytes':size,'sha256':digest.hexdigest(),'zip_members':len(members),'uncompressed_bytes':sum(x.file_size for x in members),'uncompressed_bytes_by_day':days,'verification':'exact expected size, local SHA256 recorded, ZIP directory readable; full CRC scan not yet performed'}
        (logs/(table+'.json')).write_text(json.dumps(evidence,indent=2))
        status['archives'][table].update(evidence,state='complete');save()
    status['state']='complete';status['finished_at']=time.time();save()
except BaseException as e:
    status['state']='failed';status['error']=repr(e);save();raise
PY
chown ec2-user:ec2-user /opt/gpu-fleet/download-archives.py
systemd-run --unit=gpu-fleet-download --uid=ec2-user --property=WorkingDirectory=/opt/gpu-fleet --property=RuntimeMaxSec=6600 /opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/download-archives.py
systemctl is-active gpu-fleet-stop.timer
systemctl status gpu-fleet-download --no-pager
df -h /data/gpu-fleet
