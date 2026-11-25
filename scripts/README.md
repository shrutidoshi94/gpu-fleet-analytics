# Local setup and verification

Run commands from the repository root.

| Tool | Purpose | AWS required? |
|---|---|---|
| `download_gold.py` | Download a release once, verify staging, then install locally | Yes, setup only |
| `verify_gold_bundle.py` | Recheck saved files, schemas and reconciled totals | No |
| `package_gold.py` | Package a verified local bundle for a GitHub Release | No |
| `verify_evidence.py` | Check committed AWS records and processing-code identities | No |

```bash
python -m scripts.verify_evidence
python -m scripts.verify_gold_bundle data/gold/gold_20261120
python -m scripts.download_gold --bucket YOUR_BUCKET --profile gpu-fleet
```

Synthetic fixtures and the earlier pilot live in [examples](../examples/README.md). Worker-side operations live in [ops](../ops/README.md).
