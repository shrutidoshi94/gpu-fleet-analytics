set -euo pipefail
# Install the fallback before replacing the shorter timer for this boot.
shutdown -h +720 'GPU fleet archive download session limit'
mkdir -p /run/systemd/system/gpu-fleet-download.service.d
cat > /run/systemd/system/gpu-fleet-download.service.d/runtime.conf <<'EOF'
[Service]
RuntimeMaxSec=11h
ExecStopPost=+/usr/sbin/shutdown -h now
EOF
systemctl daemon-reload
systemctl stop gpu-fleet-stop.timer
systemctl show gpu-fleet-download.service -p ActiveState -p RuntimeMaxUSec -p ExecStopPost
shutdown --show
