# Airflow orchestration

| DAG file | Role |
|---|---|
| `gpu_silver.py` | Production Bronze → Silver backfill |
| `gpu_gold.py` | Production Silver → Gold daily aggregates and global context |
| `gpu_trace.py`, `gpu_backfill.py` | Earlier ingestion/pilot workflow |

Airflow schedules work; Spark performs transformations. Production used an isolated DAG directory containing the Silver and Gold DAGs. Follow the [runbooks](../docs/runbooks/README.md) rather than loading every historical DAG onto a worker.
