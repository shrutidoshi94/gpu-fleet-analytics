#!/usr/bin/env bash
# Format only an explicitly identified, blank archive EBS device.
set -euo pipefail
VOLUME_ID="${1:?Supply the archive volume ID from the separate stack}"
[[ "$VOLUME_ID" =~ ^vol-[0-9a-f]+$ ]] || { echo 'Invalid volume ID' >&2; exit 1; }
SERIAL="${VOLUME_ID//-/}"
DEVICE=''
for candidate in /dev/nvme*n1; do
  [[ "$(lsblk -dn -o SERIAL "$candidate" | tr -d ' ')" == "$SERIAL" ]] && DEVICE="$candidate"
done
[[ -n "$DEVICE" ]] || { echo 'The requested volume is not attached' >&2; exit 1; }
[[ "$(blockdev --getsize64 "$DEVICE")" -ge 429496729600 ]]
if ! blkid "$DEVICE" >/dev/null; then
  [[ "$(lsblk -nr "$DEVICE" | wc -l)" -eq 1 ]]
  [[ -z "$(wipefs -n --noheadings "$DEVICE")" ]]
  mkfs.xfs "$DEVICE"
fi
[[ "$(blkid -s TYPE -o value "$DEVICE")" == xfs ]]
mkdir -p /data/gpu-fleet
UUID="$(blkid -s UUID -o value "$DEVICE")"
if ! grep -q "UUID=$UUID " /etc/fstab; then
  printf 'UUID=%s /data/gpu-fleet xfs defaults,nofail 0 2\n' "$UUID" >> /etc/fstab
fi
mountpoint -q /data/gpu-fleet || mount /data/gpu-fleet
[[ "$(findmnt -n -o UUID /data/gpu-fleet)" == "$UUID" ]]
mkdir -p /data/gpu-fleet/archives
chown ec2-user:ec2-user /data/gpu-fleet/archives
