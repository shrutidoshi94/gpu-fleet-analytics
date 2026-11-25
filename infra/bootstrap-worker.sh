#!/usr/bin/env bash
# Run as root on a NEW Amazon Linux 2023 worker after extracting the source archive.
set -euo pipefail
: "${TRACE_BUCKET:?Set the output bucket name}"
PROJECT_DIR="${PROJECT_DIR:-/opt/gpu-fleet}"
WORKER_USER="${WORKER_USER:-ec2-user}"
cd "$PROJECT_DIR"
if [[ -e production.env.sh ]]; then
  echo "Refusing to overwrite an existing worker configuration." >&2
  exit 1
fi
dnf install -y python3.11 python3.11-pip java-17-amazon-corretto-headless postgresql15-server git tar
if [[ ! -f /var/lib/pgsql/data/PG_VERSION ]]; then
  postgresql-setup --initdb
fi
systemctl enable --now postgresql
# Enable password authentication on loopback for the application role.
python3.11 - <<'PYCODE'
from pathlib import Path
p=Path('/var/lib/pgsql/data/pg_hba.conf')
s=p.read_text()
s='host airflow airflow 127.0.0.1/32 scram-sha-256\n'+s
p.write_text(s)
PYCODE
systemctl reload postgresql
DB_PASSWORD="$(python3.11 -c 'import secrets;print(secrets.token_hex(32))')"
API_SECRET="$(python3.11 -c 'import secrets;print(secrets.token_hex(32))')"
printf "CREATE ROLE airflow LOGIN PASSWORD '%s';\n" "$DB_PASSWORD" | runuser -u postgres -- psql --set ON_ERROR_STOP=1
runuser -u postgres -- createdb --owner=airflow airflow
chown -R "$WORKER_USER:$WORKER_USER" "$PROJECT_DIR"
runuser -u "$WORKER_USER" -- python3.11 -m venv .venv
runuser -u "$WORKER_USER" -- .venv/bin/pip install -r app/requirements.txt -r pipelines/requirements.txt
runuser -u "$WORKER_USER" -- .venv/bin/pip install 'apache-airflow[postgres]==3.0.6' --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.11.txt
runuser -u "$WORKER_USER" -- .venv/bin/pip install --no-deps -e .
runuser -u "$WORKER_USER" -- .venv/bin/pip check
mkdir -p production-dags logs airflow-production
cp dags/gpu_silver.py dags/gpu_gold.py production-dags/
umask 077
cat > production.env.sh <<ENV
export TRACE_PROJECT='$PROJECT_DIR'
export TRACE_BUCKET='$TRACE_BUCKET'
export TRACE_SILVER_ROOT='s3://$TRACE_BUCKET'
export TRACE_SILVER_STATUS_KEY='run-logs/silver/status.json'
export AWS_DEFAULT_REGION='${AWS_DEFAULT_REGION:-us-east-2}'
export TRACE_SPARK_MASTER='local[6]'
export TRACE_OUTPUT_PARTITIONS='4'
export SPARK_LOCAL_IP='127.0.0.1'
export PYSPARK_PYTHON='$PROJECT_DIR/.venv/bin/python'
export PYSPARK_SUBMIT_ARGS='--driver-memory 12g pyspark-shell'
export AIRFLOW_HOME='$PROJECT_DIR/airflow-production'
export AIRFLOW__CORE__DAGS_FOLDER='$PROJECT_DIR/production-dags'
export AIRFLOW__CORE__EXECUTOR='LocalExecutor'
export AIRFLOW__CORE__LOAD_EXAMPLES='false'
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://airflow:$DB_PASSWORD@127.0.0.1/airflow'
export AIRFLOW__API__SECRET_KEY='$API_SECRET'
export AIRFLOW__API_AUTH__JWT_SECRET='$API_SECRET'
export PATH='$PROJECT_DIR/.venv/bin:/usr/local/bin:/usr/bin:/bin'
ENV
chown -R "$WORKER_USER:$WORKER_USER" production.env.sh production-dags logs airflow-production
runuser -u "$WORKER_USER" -- bash -c 'set -a; source production.env.sh; airflow db migrate'
for component in scheduler dag-processor api; do
  case "$component" in
    api) launch='api-server --host 127.0.0.1 --port 8080' ;;
    *) launch="$component" ;;
  esac
  cat > "/etc/systemd/system/gpu-airflow-$component.service" <<UNIT
[Unit]
Description=GPU fleet Airflow $component
After=network-online.target postgresql.service
Requires=postgresql.service
[Service]
User=$WORKER_USER
WorkingDirectory=$PROJECT_DIR
ExecStart=/bin/bash -c 'source $PROJECT_DIR/production.env.sh; exec airflow $launch'
Restart=on-failure
RestartSec=10
[Install]
WantedBy=multi-user.target
UNIT
done
systemctl daemon-reload
systemctl enable --now gpu-airflow-scheduler gpu-airflow-dag-processor gpu-airflow-api
systemctl is-active gpu-airflow-scheduler gpu-airflow-dag-processor gpu-airflow-api
systemctl is-active gpu-fleet-stop.timer
printf 'Setup complete. No ingestion or transformation DAG was triggered.\n'
