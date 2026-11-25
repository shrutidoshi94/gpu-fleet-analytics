set -euo pipefail
systemctl show gpu-fleet-download.service -p ActiveState -p RuntimeMaxUSec -p ExecStopPost
systemctl stop gpu-fleet-stop.timer
shutdown --show
