set -euo pipefail
systemctl start gpu-fleet-stop.timer
systemctl show gpu-fleet-stop.timer -p ActiveState -p NextElapseUSecMonotonic
systemctl cat gpu-fleet-stop.timer gpu-fleet-stop.service
findmnt /data || true
lsblk -o NAME,SIZE,TYPE,MOUNTPOINTS
df -h /opt/gpu-fleet
free -h
nproc
systemctl list-units --type=service --state=running --no-pager | head -25
aws s3 cp s3://__BUCKET__/source/silver-benchmark-20261119.tar.gz /opt/gpu-fleet/silver-benchmark.tar.gz --only-show-errors
tar -xzf /opt/gpu-fleet/silver-benchmark.tar.gz -C /opt/gpu-fleet
chown ec2-user:ec2-user /opt/gpu-fleet/silver.py /opt/gpu-fleet/examples/benchmark_silver.py /opt/gpu-fleet/examples/estimate_silver.py /opt/gpu-fleet/dags/gpu_silver.py
runuser -u ec2-user -- bash <<'USER'
set -euo pipefail
cd /opt/gpu-fleet
source env.sh
python -c 'import pyspark,pyarrow,boto3; print("pyspark",pyspark.__version__,"pyarrow",pyarrow.__version__,"boto3",boto3.__version__)'
python -m pip check
java -version
python -m py_compile silver.py examples/benchmark_silver.py examples/estimate_silver.py
USER
