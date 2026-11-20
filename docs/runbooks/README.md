# Runbooks

[Silver runbook](SILVER_RUNBOOK.md) contains contracts, local validation and publication rules. Its historical benchmark sections are explicitly dated.

For the current AWS run, read [HANDOFF](../../HANDOFF.md) first. Use `dags/gpu_silver.py` and `ops/silver_supervisor.py`; do not invoke the legacy Gold-producing pilot DAGs. Scripts under `infra/scripts/` are deployment-specific records, not a turnkey deployment. The original CloudFormation template predates the approved CPU/disk resize; review drift before applying it.

[Gold runbook](GOLD_RUNBOOK.md): deployed Gold DAG, verified-Silver handoff, pilot budget gate, publication and portable export. The Gold design also identifies dashboard features still pending.
