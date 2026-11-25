# Worker operations

These programs ran on EC2. Local dashboard use does not require them.

| Files | Responsibility |
|---|---|
| `silver_task.py`, `gold_task.py` | Run a planned transformation unit |
| `silver_supervisor.py`, `gold_supervisor.py` | Track progress, enforce guards and verify completion |
| `export_gold.py` | Consolidate Gold into the portable dashboard bundle |
| `profile_bronze.py` | Inspect source metadata and data-quality conditions |

[Silver runbook](../docs/runbooks/SILVER_RUNBOOK.md) · [Gold runbook](../docs/runbooks/GOLD_RUNBOOK.md) · [Execution evidence](../docs/evidence/README.md)

Historical deployment paths are retained for reproducibility. Read the [runbooks](../docs/runbooks/README.md) and [infrastructure notes](../infra/README.md) before operating AWS resources.
