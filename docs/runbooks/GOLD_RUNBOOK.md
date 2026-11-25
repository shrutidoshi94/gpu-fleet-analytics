# Silver → Gold

Release `gold_20261120` completed 185 daily reductions plus global context in 8h52m and exported seven Parquet tables. The portable bundle contains 47,379,950 bytes of tables/metadata plus its manifest. See [completion evidence](../evidence/README.md).

## Inputs and outputs

Gold pins accepted Silver manifests and their code/source identities. It reduces pod rows by server-hour before joining unique inventory, preventing capacity fanout. It writes hourly capacity, activity, workload mix and quality, daily placement bins, network context for days 109–115 and global execution context. [Dictionary](../data/GOLD_DATA_DICTIONARY.md) · [Design](../design/GOLD_TRANSFORMATION_PLAN.md).

Source validity and inventory matching determine which metrics are calculable. A repeated pod candidate-key group marks its entire day for conservative screening; that is a policy choice, not proof of erroneous duplication. The dashboard compares screened and unscreened pressure. Missing readings stay unknown, activity stays on its source scale and scenarios require a demand assumption.

## Run

Use a configured worker from the [infrastructure guide](../../infra/README.md), source `production.env.sh` and run from the checkout root. Bronze/Silver must already exist. Production pinning requires the complete Silver status checkpoint; local fixtures require the explicit synthetic mode.

```bash
python -m pipelines.gold pin --root s3://YOUR_BUCKET --release YOUR_NEW_RELEASE
python -m pipelines.gold day --root s3://YOUR_BUCKET --release YOUR_NEW_RELEASE --day 109
python -m pipelines.gold context --root s3://YOUR_BUCKET --release YOUR_NEW_RELEASE
# Complete every pinned day before publishing.
python -m pipelines.gold publish --root s3://YOUR_BUCKET --release YOUR_NEW_RELEASE
python -m ops.export_gold --root s3://YOUR_BUCKET --release YOUR_NEW_RELEASE --destination /tmp/gold-bundle
```

For orchestration, trigger `gpu_gold_backfill` with a new release name and the desired `days`. The DAG selects a pilot from those days, checks measured runtime, processes remaining days serially, builds global context, then publishes/exports. Single-day and pilot-only selections are supported for partition checks. The production dashboard expects the full seven-table release; a partial release may omit the network table. Pilot reports must match the release, selected days and pinned plan; stale reports cannot satisfy the gate.

## Recovery

The worker task enforces a one-hour child deadline and a 10-GiB free-space floor; day tasks have a 75-minute deadline and one retry. The boot shutdown timer is an independent outer bound. Inspect task logs and worker state after a failure.

Units are reused only when plan, code and output inventories match. Pilot forecasting requires matching measured task reports; missing timing cannot be treated as a zero-duration run. Publication rechecks every pinned source and output inventory. A failed attempt leaves the preceding unit pointer intact; use a new release when source/code changes.

Export writes to staging, verifies hashes, then installs the portable bundle. [Dashboard setup](DASHBOARD.md) describes the GitHub release package and hosted startup. Keep the original run evidence and a verified data backup before cloud teardown.
