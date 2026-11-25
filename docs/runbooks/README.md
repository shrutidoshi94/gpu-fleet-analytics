# Runbooks

| Task | Instructions |
|---|---|
| Serve and deploy the saved-data dashboard | [Dashboard](DASHBOARD.md) |
| Reproduce Bronze → Silver processing | [Silver](SILVER_RUNBOOK.md) |
| Reproduce Silver → Gold and export the bundle | [Gold](GOLD_RUNBOOK.md) |

Silver and Gold completed on AWS; see the [execution evidence](../evidence/README.md). The [infrastructure notes](../../infra/README.md) record the environment and deployment limits. The offline dashboard requires no running EC2 worker.

Production workflows are `dags/gpu_silver.py` and `dags/gpu_gold.py`. Earlier pilot DAGs produce different, narrower outputs. Historical benchmark sections and scripts describe their dated execution environment; inspect run IDs, placeholders and template drift before reuse.
