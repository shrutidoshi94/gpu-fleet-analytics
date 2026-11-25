#!/usr/bin/env bash
# Explicit ingestion job. Use the temporary archive disk for the full trace.
set -euo pipefail
cd "${PROJECT_DIR:-/opt/gpu-fleet}"
source production.env.sh
ARCHIVE_DIR="${1:?Supply the archive directory on the mounted temporary disk}"
mkdir -p "$ARCHIVE_DIR" logs/ingestion
for item in network:asi_opensource_network_hourly:203904100 server:asi_opensource_server_hourly:3080121387 execution:asi_opensource_job_execution_summary:1188295031 pod:asi_opensource_pod_hourly:351803513445; do
  IFS=: read -r dataset table expected <<< "$item"
  archive="$ARCHIVE_DIR/$table.zip"
  python - "$archive" "$expected" <<'PY'
import pathlib,shutil,sys
path=pathlib.Path(sys.argv[1]);expected=int(sys.argv[2])
existing=path.stat().st_size if path.exists() else 0
if existing>expected:raise SystemExit('Archive exceeds recorded size; review upstream version')
if shutil.disk_usage(path.parent).free < expected-existing+20*2**30:
    raise SystemExit('Insufficient archive storage with 20-GiB reserve')
PY
  existing=0
  [[ ! -f "$archive" ]] || existing="$(stat -c %s "$archive")"
  if [[ "$existing" -lt "$expected" ]]; then
    curl --fail --location --continue-at - --retry 5 --connect-timeout 30 \
      --output "$archive" "https://tre-clusterdata.oss-cn-hangzhou.aliyuncs.com/cluster-trace-gpu-v2026/data/$table.zip"
  fi
  [[ "$(stat -c %s "$archive")" == "$expected" ]]
  sha256sum "$archive" >> logs/ingestion/archive-sha256.txt
  python -m pipelines.ingest_bronze "$dataset" --archive-dir "$ARCHIVE_DIR" --bucket "$TRACE_BUCKET"
done
python -m ops.profile_bronze --root "$TRACE_SILVER_ROOT" --output logs/ingestion/bronze-profile.json
aws s3 cp logs/ingestion/bronze-profile.json "s3://$TRACE_BUCKET/manifests/bronze/quality-observations.json" --only-show-errors
printf 'Verified Bronze and source observation catalog are ready.\n'
