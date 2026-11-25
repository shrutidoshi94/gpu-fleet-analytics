set -euo pipefail
# Refuse to start a benchmark with less than 10 GiB scratch headroom.
python3 - <<'PY'
import shutil
assert shutil.disk_usage('/opt/gpu-fleet').free >= 10*2**30, 'Insufficient free root space'
PY
mkdir -p /opt/gpu-fleet/logs/silver-benchmark-20261119
chown ec2-user:ec2-user /opt/gpu-fleet/logs/silver-benchmark-20261119
cat > /opt/gpu-fleet/run-silver-benchmark.sh <<'RUN'
set -euo pipefail
cd /opt/gpu-fleet
source env.sh
export AWS_DEFAULT_REGION=us-east-2
export TRACE_SILVER_ROOT=s3://__BUCKET__
export TRACE_SPARK_MASTER='local[6]'
export PYSPARK_SUBMIT_ARGS='--driver-memory 12g pyspark-shell'
resume_args=()
if [ -f /opt/gpu-fleet/logs/silver-benchmark-20261119/results.json ]; then
    # Keep the first attempt's failure evidence. Reuse only validated cases.
    for name in runner.log service-result.json; do
        file=/opt/gpu-fleet/logs/silver-benchmark-20261119/$name
        if [ -f "$file" ]; then cp "$file" "$file.prior-$(date +%s)"; fi
    done
    resume_args=(--resume)
else
    # A cloud Airflow smoke run on one small partition, not a full backfill.
    airflow dags test gpu_silver_backfill 2026-11-19 -c '{"datasets":["server"],"days":[3]}' > /opt/gpu-fleet/logs/silver-benchmark-20261119/airflow-smoke.log 2>&1
fi
exec python examples/benchmark_silver.py --root s3://__BUCKET__ --results /opt/gpu-fleet/logs/silver-benchmark-20261119 --threads 6 --driver-memory 12g --max-seconds 5400 --min-free-gib 10 "${resume_args[@]}" > /opt/gpu-fleet/logs/silver-benchmark-20261119/runner.log 2>&1
RUN
cat > /opt/gpu-fleet/finalize-silver-benchmark.sh <<'FINAL'
set +e
export AWS_DEFAULT_REGION=us-east-2
result_dir=/opt/gpu-fleet/logs/silver-benchmark-20261119
printf '{"service_result":"%s","exit_code":"%s","exit_status":"%s"}\n' "${SERVICE_RESULT:-unknown}" "${EXIT_CODE:-unknown}" "${EXIT_STATUS:-unknown}" > "$result_dir/service-result.json"
journalctl -u gpu-silver-benchmark --no-pager > "$result_dir/systemd.log"
df -h > "$result_dir/disk-final.txt"
free -h > "$result_dir/memory-final.txt"
aws s3 sync "$result_dir" s3://__BUCKET__/run-logs/silver-benchmark-20261119/ --only-show-errors
sync_status=$?
printf 'Log sync exit: %s\n' "$sync_status"
/usr/sbin/shutdown -h now
exit "$sync_status"
FINAL
cat > /etc/systemd/system/gpu-silver-benchmark.service <<'UNIT'
[Unit]
Description=Bounded GPU trace Silver benchmark only
After=network-online.target
[Service]
Type=exec
User=ec2-user
WorkingDirectory=/opt/gpu-fleet
ExecStart=/bin/bash /opt/gpu-fleet/run-silver-benchmark.sh
ExecStopPost=+/bin/bash /opt/gpu-fleet/finalize-silver-benchmark.sh
RuntimeMaxSec=100m
TimeoutStopSec=90
KillMode=control-group
UNIT
chown ec2-user:ec2-user /opt/gpu-fleet/run-silver-benchmark.sh
systemctl daemon-reload
systemctl reset-failed gpu-silver-benchmark.service || true
systemctl start gpu-silver-benchmark.service
systemctl show gpu-silver-benchmark.service -p ActiveState -p MainPID -p RuntimeMaxUSec -p ExecStopPost
