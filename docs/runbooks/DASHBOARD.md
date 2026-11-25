# Offline Streamlit dashboard

Implemented November23,2026. Production release: gold_20261120. No AWS clients, credentials or network reads are used during dashboard interaction. EC2 stays stopped.

## Install and run

Use Python3.11 and `pip install -r requirements-dashboard.txt`, then `streamlit run dashboard.py`. Default data directory: `data/gold/gold_20261120/`, relative to the launch directory. `GOLD_BUNDLE_DIR` can point to another local folder. Start from the repo root. The existing synthetic JSON pilot is retained when no production bundle is found and `output/gold/*/*/summary.json` exists.

For a clean synthetic Gold demo, run `python examples/make_dashboard_sample.py /tmp/your-empty-demo-folder`, then launch with `GOLD_BUNDLE_DIR=/tmp/your-empty-demo-folder`. The UI labels synthetic fixtures explicitly; they are not AWS execution evidence.

## One-time download

```bash
aws login --profile gpu-fleet --region us-east-2
python examples/download_gold.py --bucket YOUR_BUCKET --profile gpu-fleet
python examples/verify_gold_bundle.py data/gold/gold_20261120
```

The downloader uses AWS CLI only during setup, verifies a staging directory, then publishes the local folder. An existing bundle is verified/reused, not overwritten. No EC2 restart is required. The47.4-MB production bundle is installed locally; a second downloaded copy remains in this chat workspace. Those copies share a laptop and are not an independent disaster-recovery backup.

Checks:9 exported file SHA256 hashes; seven Parquet schemas/row counts; Arrow sums for185 days against pinned Gold inventory totals;6,570,124,684 workload observations reconciled against source acceptance reports; all five mix dimensions reconcile to that population. This independently checks Gold consolidation, not a fresh reprocessing of raw Bronze/Silver. See `docs/evidence/local-gold-verification.json`.

## Story and contracts

The main page has three short single-line headings: Find the pressure, Review existing capacity, Test the requirement. A selected cohort drives every step. Each step leads with a finding; supporting tables, source scopes and lineage are inside Data and definitions. Settings and hourly evidence are optional drill-downs. No competing page tabs remain.

The metric contracts below still apply:

| View | Decision support | Interpretation |
|---|---|---|
| Capacity | Inventory/request trend, p95 concurrent request evidence, cohort pressure and coverage | Sum within each hour before calculating percentiles; no sum of cohort percentiles. Inventory remains visible across excluded request periods. Gaps stay gaps. |
| Activity | Configurable low-activity/persistence/coverage review; workload context | SM is in source units, not fleet GPU utilization. Means use sums/valid counts. Review hours are not reclaim amounts. |
| Placement | Server-hour headroom bins by cohort, unknown coverage and over-requested hours | Inventory is counted once. Gold puts incomplete/anomalous/duplicate-candidate evidence into unknown bins. No device-level fragmentation or safe spare-GPU claim. |
| Scenarios | One-cohort growth/reserve sensitivity with explicit acknowledgement | Request anchor excludes flagged observations; capacity baseline uses latest/minimum valid inventory independently. No hardware equivalence or performance sufficiency assumption. |
| Trace context | Network days109–115, global observed execution spans/delays | Network uses only the selected overlap, not zero-filled outside coverage. Execution ignores day/cluster filters and never enters daily capacity. |
| Evidence | Source scopes, quality, product rows, lineage and cluster aliases | Hashes establish snapshot consistency, not independent AWS attestation. Cluster aliases are stable; source hashes remain inspectable. |

The production data flags53 trace days for unproven duplicate candidates plus the known day5/hour06 inconsistency. Default request-based views exclude those flags; the selected window shows the number of excluded hours. Turning screening off produces a warning. Scenario anchors always exclude flags. Missing request evidence yields Unknown; zero is retained when observed. All procurement-readiness flags remain false because complete running-demand semantics have not been established.

Means/percentages retain their denominators. No averaged daily percentiles, cross-window summed distinct counts or0–100SM clamping is used. CPU-only rows remain in workload-mix context. Modeled additional slots are arithmetic under the chosen assumptions, not an order quantity.

## Validation and serving

`python -m unittest discover -s tests -p test_dashboard.py -v` exercises math, unknowns, independent inventory baselines, the three-step story, cohort continuity, supporting scopes, scenario acknowledgement, empty flagged windows and same-size data tampering. Network sockets are blocked during UI tests. A separate real-data smoke tested all pages, filters and scenarios; the old synthetic Spark/pilot UI test still passes.

The local app is served on127.0.0.1:8501. It requires the laptop while used locally. No public deployment was performed. To host it elsewhere, package the verified data with the app or provision it during deployment setup. The gitignored bundle is not included in a source-only checkout; do not add an S3 fetch to each page interaction. Optional chart/CSV exports are generated locally.

## Remaining work

Review the local dashboard, select a hosting/data-package strategy, then publish when requested. Expanded telemetry distributions or server-level drill-down require additional Gold products; existing aggregates cannot establish those results. Do not delete AWS data until a verified independent backup and explicit teardown instruction.
