set -euo pipefail
# User explicitly requested continuing to processing without a shutdown.
shutdown -c || true
systemctl stop gpu-fleet-stop.timer
cat > /run/systemd/system/gpu-fleet-download.service.d/runtime.conf <<'EOF'
[Service]
RuntimeMaxSec=11h
ExecStopPost=
ExecStopPost=+/opt/gpu-fleet/.venv/bin/python /opt/gpu-fleet/publish-download-progress.py
EOF
systemctl daemon-reload
systemctl show gpu-fleet-download.service -p ActiveState -p RuntimeMaxUSec -p ExecStopPost
systemctl is-active gpu-fleet-stop.timer || true
shutdown --show || true
