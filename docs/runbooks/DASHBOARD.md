# Offline Streamlit dashboard

Implemented November23,2026. Production release: gold_20261120. No AWS clients or credentials are used during dashboard interaction. Missing default data is fetched once at startup from GitHub; chart interactions read local files. EC2 stays stopped.

## Install and run

Use Python3.11 and `pip install -r app/requirements.txt`, then `streamlit run app/dashboard.py`. Default data directory: `data/gold/gold_20261120/`, relative to the launch directory. `GOLD_BUNDLE_DIR` can point to another local folder. Start from the repo root. The old JSON pilot is separate: run `streamlit run examples/pilot_dashboard.py` explicitly.

For a clean synthetic Gold demo, run `python -m examples.make_dashboard_sample /tmp/your-empty-demo-folder`, then launch with `GOLD_BUNDLE_DIR=/tmp/your-empty-demo-folder`. The UI labels synthetic fixtures explicitly; they are not AWS execution evidence.

## One-time download

```bash
aws login --profile gpu-fleet --region us-east-2
python -m scripts.download_gold --bucket YOUR_BUCKET --profile gpu-fleet
python -m scripts.verify_gold_bundle data/gold/gold_20261120
```

The downloader uses AWS CLI only during setup, verifies a staging directory, then publishes the local folder. An existing bundle is verified/reused, not overwritten. No EC2 restart is required. The production archive is also published as the pinned GitHub data release; local and hosted installs verify its hashes.

Checks:9 exported file SHA256 hashes; seven Parquet schemas/row counts; Arrow sums for185 days against pinned Gold inventory totals;6,570,124,684 workload observations reconciled against source acceptance reports; all five mix dimensions reconcile to that population. This independently checks Gold consolidation, not a fresh reprocessing of raw Bronze/Silver. See `docs/evidence/local-gold-verification.json`.

## Story and contracts

Three numbered tabs form the decision journey: Demand pressure, Existing GPUs, Plan for growth. Click a GPU bar in step 1 to carry that selection through all tabs; no duplicate GPU dropdown. Types without screened requests remain selectable with buttons. Activity shows measured values and missing coverage without an arbitrary low-activity threshold. Scenario choices use visible radio controls. Data and definitions remains a shared supporting expander.

The metric contracts below still apply:

| View | Decision support | Interpretation |
|---|---|---|
| Capacity | Inventory/request trend, p95 hourly request evidence, cohort pressure and coverage | Sum within each hour before calculating percentiles; no sum of cohort percentiles. Inventory remains visible across excluded request periods. Gaps stay gaps. |
| Activity | Measured activity trend and missing measurement coverage | SM is in source units, not fleet GPU utilization. Means use sums/valid counts. Review hours are not reclaim amounts. |
| Placement | Server-hour headroom bins by cohort, unknown coverage and over-requested hours | Inventory is counted once. Gold puts incomplete/anomalous/duplicate-candidate evidence into unknown bins. No device-level fragmentation or safe spare-GPU claim. |
| Scenarios | One-cohort growth/reserve sensitivity with explicit acknowledgement | Request anchor excludes flagged observations; capacity baseline uses latest/minimum valid inventory independently. No hardware equivalence or performance sufficiency assumption. |
| Trace context | Network days109–115, global observed execution spans/delays | Network uses only the selected overlap, not zero-filled outside coverage. Execution ignores day/cluster filters and never enters daily capacity. |
| Evidence | Source scopes, quality, product rows, lineage and cluster aliases | Hashes establish snapshot consistency, not independent AWS attestation. Cluster aliases are stable; source hashes remain inspectable. |

The production data flags53 trace days for unproven duplicate candidates plus the known day5/hour06 inconsistency. Default request-based views exclude those flags; the selected window shows the number of excluded hours. Turning screening off produces a warning. Scenario anchors always exclude flags. Missing request evidence yields Unknown; zero is retained when observed. All procurement-readiness flags remain false because complete running-demand semantics have not been established.

Means/percentages retain their denominators. No averaged daily percentiles, cross-window summed distinct counts or0–100SM clamping is used. CPU-only rows remain in workload-mix context. Modeled additional slots are arithmetic under the chosen assumptions, not an order quantity.

## Validation and serving

`python -m unittest discover -s tests -p test_dashboard.py -v` exercises math, unknowns, independent inventory baselines, the three-step story, cohort continuity, supporting scopes, scenario acknowledgement, empty flagged windows and same-size data tampering. Network sockets are blocked during UI tests. A separate real-data smoke tested all pages, filters and scenarios; the old synthetic Spark/pilot UI test still passes.

The local app is served on127.0.0.1:8501. It requires the laptop while used locally. For hosted startup, `app/gold_release.json` pins the public release URL, archive SHA256 and compressed/expanded sizes. The app downloads only when the default bundle is absent, validates the archive and all bundled files, reconciles inventory/workload totals, then installs atomically. Streamlit resource caching shares setup across sessions. The host may download again after a rebuild or local disk reset. No visitor interaction fetches S3 data. Optional chart/CSV exports are generated locally.

## Remaining work

The public Gold data release supports hosted startup. Changes to dashboard code require a normal Streamlit redeploy. Expanded telemetry distributions or server-level drill-down require additional Gold products; existing aggregates cannot establish those results. Do not delete AWS data until a verified independent backup and explicit teardown instruction.

## Community Cloud runtime

Use Python **3.11** in deployment Advanced settings. Streamlit 1.48.1 / Altair 5.5.0 are locally tested on that runtime. The Altair 5 import path can raise `TypeError: _TypedDictMeta.__new__() got an unexpected keyword argument 'closed'` on Python 3.14; the subsequent script-health-check 503 reports that startup failure.

Community Cloud reads `app/requirements.txt` beside the `app/dashboard.py` entrypoint. Changing Python requires deleting and redeploying the hosted app; save the repository/branch/entrypoint, custom URL and any secrets first. Select Python 3.11 in Advanced settings before deploying. See [Streamlit runtime instructions](https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app/upgrade-python).

Set entrypoint to `app/dashboard.py`. No Gold environment setting is needed for the default GitHub release setup. If `GOLD_BUNDLE_DIR` is explicitly set, that folder must already contain a verified bundle; the app will not silently substitute another dataset. On a transient download failure, reboot the app to retry. `tests/test_bundle_setup.py` covers first installation, reuse without another download and rejection of corrupted archives.

## Imports after a source update

If hosted startup reports a missing helper that exists in the current source, use Manage app → Reboot app to clear the old process. A source rerun may retain an older imported module during a structural update. Dashboard dependencies are pinned to the tested versions; changing the dependency file also triggers a fresh Community Cloud deployment. See [Streamlit reboot instructions](https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app/reboot-your-app).
