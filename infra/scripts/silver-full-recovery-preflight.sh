set -euo pipefail
if ! systemctl is-active --quiet gpu-recovery-stop.timer; then systemd-run --unit=gpu-recovery-stop --on-active=30m --timer-property=AccuracySec=1s /usr/sbin/shutdown -h now; fi
aws s3 cp s3://__BUCKET__/source/full-silver-recovery.tar.gz /opt/gpu-fleet/full-silver-recovery.tar.gz --only-show-errors
tar -xzf /opt/gpu-fleet/full-silver-recovery.tar.gz -C /opt/gpu-fleet
cp /opt/gpu-fleet/dags/gpu_silver.py /opt/gpu-fleet/production-dags/
chown -R ec2-user:ec2-user /opt/gpu-fleet/ops /opt/gpu-fleet/production-dags
chown ec2-user:ec2-user /opt/gpu-fleet/silver.py
# Verify an independent shutdown-action timer survives a timed-out cleanup.
probe_dir=$(mktemp -d /tmp/gpu-cleanup.XXXXXX)
probe_id=$(date +%s)
cat > /opt/gpu-fleet/cleanup-probe.sh <<PROBE
systemd-run --unit=gpu-cleanup-watchdog-$probe_id --on-active=3s --timer-property=AccuracySec=1s /usr/bin/touch "$probe_dir/passed"
sleep 20
PROBE
systemd-run --unit=gpu-cleanup-parent-$probe_id --property=TimeoutStopSec=2 --property='ExecStopPost=/bin/bash /opt/gpu-fleet/cleanup-probe.sh' /bin/false
sleep 7
test -f "$probe_dir/passed"
echo 'Independent cleanup watchdog probe passed'
systemctl show gpu-cleanup-parent-$probe_id -p ActiveState -p Result
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source production.env.sh
python ops/profile_bronze.py --root s3://__BUCKET__ --output logs/full-silver-20261119/whole-bronze-preflight.json > logs/full-silver-20261119/whole-bronze-preflight.log 2>&1
python - <<'PUBLISH_CATALOG'
import json
from pathlib import Path
from silver import Store
r=json.loads(Path('logs/full-silver-20261119/whole-bronze-preflight.json').read_text())
assert r['status']=='passed' and not r['errors']
Store('s3://__BUCKET__').write('manifests/bronze/quality-observations.json',{'method':r['method'],'observations':r['observations']})
print('Published source-quality observations:',len(r['observations']))
PUBLISH_CATALOG
aws s3 cp logs/full-silver-20261119/whole-bronze-preflight.json s3://__BUCKET__/run-logs/full-silver-20261119/whole-bronze-preflight.json --only-show-errors
USER
