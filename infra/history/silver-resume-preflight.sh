set -euo pipefail
systemctl start gpu-fleet-stop.timer
systemctl show gpu-fleet-stop.timer -p ActiveState -p NextElapseUSecMonotonic
lsblk -b -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS
root_disk_bytes=$(lsblk -bdno SIZE /dev/nvme0n1)
root_part_bytes=$(lsblk -bdno SIZE /dev/nvme0n1p1)
if [ "$root_disk_bytes" -lt 59000000000 ]; then echo 'Expanded device is not ready'; exit 1; fi
if [ "$root_part_bytes" -lt $((root_disk_bytes - 134217728)) ]; then growpart /dev/nvme0n1 1; fi
root_fs=$(findmnt -no FSTYPE /)
case "$root_fs" in
 xfs) xfs_growfs / ;;
 ext4) resize2fs /dev/nvme0n1p1 ;;
 *) echo "Unsupported root filesystem: $root_fs"; exit 1 ;;
esac
python3 - <<'PY'
import shutil
s=shutil.disk_usage('/opt/gpu-fleet')
assert s.total > 55*2**30, s
assert s.free > 35*2**30, s
print('Root capacity/free GiB:',s.total/2**30,s.free/2**30)
PY
aws s3 cp s3://__BUCKET__/source/silver-benchmark-resume.tar.gz /opt/gpu-fleet/silver-benchmark-resume.tar.gz --only-show-errors
tar -xzf /opt/gpu-fleet/silver-benchmark-resume.tar.gz -C /opt/gpu-fleet
chown ec2-user:ec2-user /opt/gpu-fleet/silver.py /opt/gpu-fleet/examples/benchmark_silver.py /opt/gpu-fleet/examples/estimate_silver.py /opt/gpu-fleet/dags/gpu_silver.py
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source env.sh
python -m pip check
python - <<'PY'
import json
from pathlib import Path
from examples.benchmark_silver import reusable
from silver import Store,CODE_SHA256
reports=json.loads(Path('logs/silver-benchmark-20261119/results.json').read_text())['reports']
store=Store('s3://__BUCKET__')
for r in reports:
 assert reusable(store,r,'12g'), r['benchmark_case']
 print('Validated existing case:',r['benchmark_case'])
assert len(reports)==5, len(reports)
print('Transformation SHA256:',CODE_SHA256)
PY
USER
df -h /opt/gpu-fleet
free -h
