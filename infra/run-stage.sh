#!/usr/bin/env bash
# Invoke on the configured worker as root; no stage starts during bootstrap.
set -euo pipefail
INTERNAL=0
if [[ "${1:-}" == --execute-stage ]]; then
  INTERNAL=1
  shift
fi
STAGE="${1:?Usage: run-stage.sh silver|gold NEW_RUN_OR_RELEASE_ID}"
RUN_ID="${2:?Supply a new run or release ID}"
PROJECT_DIR="${PROJECT_DIR:-/opt/gpu-fleet}"
[[ "$RUN_ID" =~ ^[a-zA-Z0-9_-]+$ ]] || { echo 'Invalid run/release ID' >&2; exit 1; }
case "$STAGE" in silver) DEADLINE=31h ;; gold) DEADLINE=16h ;; *) echo 'Select silver or gold' >&2; exit 1 ;; esac
[[ -f "$PROJECT_DIR/production.env.sh" ]] || { echo 'Bootstrap the worker first' >&2; exit 1; }
if [[ "$INTERNAL" == 1 ]]; then
  cd "$PROJECT_DIR"
  source production.env.sh
  if [[ "$STAGE" == silver ]]; then
    export TRACE_RUN_LOG_DIR="$TRACE_PROJECT/logs/silver/$RUN_ID"
    CONFIG="$(python -c 'import json; print(json.dumps({"datasets":["server","pod","network","execution"],"days":list(range(185))}))')"
    airflow dags unpause gpu_silver_backfill
    airflow dags trigger gpu_silver_backfill --run-id "$RUN_ID" --conf "$CONFIG"
    exec python -m ops.silver_supervisor --run-id "$RUN_ID"
  fi
  export TRACE_GOLD_RELEASE="$RUN_ID"
  export TRACE_GOLD_LOG_DIR="$TRACE_PROJECT/logs/gold/$RUN_ID"
  exec python -m ops.gold_supervisor
fi
systemd-run --unit="gpu-fleet-$STAGE-$RUN_ID" --uid=ec2-user \
  --property="WorkingDirectory=$PROJECT_DIR" --property="RuntimeMaxSec=$DEADLINE" \
  --property='ExecStopPost=+/usr/sbin/shutdown -h now' \
  /bin/bash "$PROJECT_DIR/infra/run-stage.sh" --execute-stage "$STAGE" "$RUN_ID"
