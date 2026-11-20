# Runbooks

[Silver runbook](SILVER_RUNBOOK.md) contains contracts, local validation and publication rules. Its historical benchmark sections are explicitly dated.

For the current AWS run, read [HANDOFF](../../HANDOFF.md) first. Use `dags/gpu_silver.py` and `ops/silver_supervisor.py`; do not invoke the legacy Gold-producing pilot DAGs. Scripts under `infra/scripts/` are deployment-specific records, not a turnkey deployment. The original CloudFormation template predates the approved CPU/disk resize; review drift before applying it.

No production Gold pipeline exists yet. Follow the [Gold plan](../design/GOLD_TRANSFORMATION_PLAN.md) for its implementation and pilot gates.
