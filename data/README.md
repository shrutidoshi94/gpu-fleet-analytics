# Local data

`gold/gold_20261120/` contains the verified production dashboard snapshot. It is gitignored. Keep `bundle.json`, its seven Parquet tables, `lineage.json` and `METRICS.md` together; do not edit manifest-listed files.

```bash
python scripts/verify_gold_bundle.py data/gold/gold_20261120
```

The app reads this saved bundle without AWS requests. A source-only clone automatically downloads and verifies the pinned GitHub Gold release on first default startup, or can use an explicitly synthetic fixture. See the [dashboard runbook](../docs/runbooks/DASHBOARD.md).
