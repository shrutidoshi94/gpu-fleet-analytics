set -euo pipefail
volume_serial=vol028dceeb16c897cfc
device=''
for candidate in /dev/nvme*n1; do
  serial=$(lsblk -dn -o SERIAL "$candidate" | tr -d ' ')
  if [ "$serial" = "$volume_serial" ]; then device=$candidate; fi
done
[ -n "$device" ]
[ "$(blockdev --getsize64 "$device")" -eq 536870912000 ]
if ! blkid "$device"; then
  [ "$(lsblk -nr "$device" | wc -l)" -eq 1 ]
  [ -z "$(wipefs -n --noheadings "$device")" ]
  mkfs.xfs "$device"
fi
[ "$(blkid -s TYPE -o value "$device")" = xfs ]
mkdir -p /data/gpu-fleet
uuid=$(blkid -s UUID -o value "$device")
if ! grep -q "UUID=$uuid " /etc/fstab; then
  printf 'UUID=%s /data/gpu-fleet xfs defaults,nofail 0 2\n' "$uuid" >> /etc/fstab
fi
mountpoint -q /data/gpu-fleet || mount /data/gpu-fleet
mkdir -p /data/gpu-fleet/{archives,scratch}
cp -n /opt/gpu-fleet/archives/asi_opensource_network_hourly.zip /data/gpu-fleet/archives/
chown -R ec2-user:ec2-user /data/gpu-fleet
cp -n /opt/gpu-fleet/env.sh /opt/gpu-fleet/env.before-data-volume.sh
sed -i 's|export TRACE_DIR=/opt/gpu-fleet/archives|export TRACE_DIR=/data/gpu-fleet/archives|; s|export SPARK_LOCAL_DIRS=/opt/gpu-fleet/scratch|export SPARK_LOCAL_DIRS=/data/gpu-fleet/scratch|; s|export TMPDIR=/opt/gpu-fleet/scratch|export TMPDIR=/data/gpu-fleet/scratch|' /opt/gpu-fleet/env.sh
runuser -u ec2-user -- bash -c 'source /opt/gpu-fleet/env.sh; touch "$SPARK_LOCAL_DIRS/.write-check"; rm "$SPARK_LOCAL_DIRS/.write-check"; test -f "$TRACE_DIR/asi_opensource_network_hourly.zip"; printf "TRACE_DIR=%s\nSPARK_LOCAL_DIRS=%s\nTMPDIR=%s\n" "$TRACE_DIR" "$SPARK_LOCAL_DIRS" "$TMPDIR"'
findmnt /data/gpu-fleet
df -h /data/gpu-fleet
systemctl is-active gpu-fleet-stop.timer
