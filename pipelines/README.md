# Transformations

| File | Role |
|---|---|
| `ingest_bronze.py` | Stage original Parquet inputs into S3 Bronze |
| `silver.py` | Full-width normalization, quality rules and versioned Silver |
| `gold.py` | Capacity-planning aggregates from pinned Silver |
| `pipeline.py` | Earlier narrow pilot; produces pilot Silver and JSON Gold |

Run scripts directly from the repository root, e.g. `python pipelines/gold.py --help`. These are sibling script modules: worker entrypoints add `pipelines/` to their module search path before importing them. The four files were moved without changing their contents, preserving the deployed Silver/Gold SHA256 identities.

[Column contracts](../docs/data/COLUMN_CONTRACTS.md) · [Gold design](../docs/design/GOLD_TRANSFORMATION_PLAN.md) · [Runbooks](../docs/runbooks/README.md)

Captured AWS logs describe the historical root-level paths. Current commands use `pipelines/`. Moving code does not modify past run evidence or require reprocessing.

`infra/scripts/` preserves historical deployment recipes that download archived source bundles using the old root-level layout. Do not apply those archived bundles as deployments of the reorganized checkout; use the current Python task entrypoints and reviewed deployment packaging.

Install processing dependencies from the repository root with `pip install -r pipelines/requirements.txt`. To run the complete regression suite (including the dashboard), also install `app/requirements.txt`. Airflow installation remains a separate orchestration setup; see the runbooks.
