set -euo pipefail
systemctl show gpu-fleet-download -p ActiveState -p SubState -p ExecMainStatus
if [ -f /data/gpu-fleet/download-logs/status.json ]; then cat /data/gpu-fleet/download-logs/status.json; fi
ls -lh /data/gpu-fleet/archives/
df -h /data/gpu-fleet
systemctl list-timers gpu-fleet-stop.timer --no-pager
