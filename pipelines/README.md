# Transformations

| File | Role |
|---|---|
| `ingest_bronze.py` | Stage original Parquet inputs into S3 Bronze |
| `silver.py` | Full-width normalization, quality rules and versioned Silver |
| `gold.py` | Capacity-planning aggregates from pinned Silver |
| `pipeline.py` | Earlier narrow pilot; produces pilot Silver and JSON Gold |

Run scripts directly from the repository root, e.g. `python -m pipelines.gold --help`. Current modules use package imports. Exact historical Silver/Gold sources are retained with the execution evidence; refactoring gives current processing code a new SHA256 identity.

[Column contracts](../docs/data/COLUMN_CONTRACTS.md) · [Gold design](../docs/design/GOLD_TRANSFORMATION_PLAN.md) · [Runbooks](../docs/runbooks/README.md)

Captured AWS logs describe the historical root-level paths. Current commands use `pipelines/`. The dashboard continues to use the historical Gold release. Existing outputs are not rewritten. To process with the refactored sources, run Silver with the current code first, then pin a new Gold release; old manifests retain their original code identities.

`infra/history/` contains old recipes. Use the current [deployment guide](../infra/README.md) for this checkout.

Install processing dependencies from the repository root with `pip install -r pipelines/requirements.txt`. To run the complete regression suite (including the dashboard), also install `app/requirements.txt`. Airflow installation remains a separate orchestration setup; see the runbooks.
