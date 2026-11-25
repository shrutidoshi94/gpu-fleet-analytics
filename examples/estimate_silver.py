"""Project Silver duration from actual benchmark results; no AWS writes.
Bounds are observed throughput scenarios with a retry allowance, NOT a
statistical confidence interval. Parallel ideal bounds are NOT forecasts.
"""

import argparse
import json
from pathlib import Path

TOTALS = {"pod": 351802613287, "server": 3079229209, "network": 203870478, "execution": 1188294827}
COUNTS = {"pod": 185, "server": 185, "network": 7, "execution": 1}


def estimate(reports, threads, retry_fraction=0.25, audit_reports=None):
    if any(r.get("synthetic_source", False) for r in reports):
        raise ValueError("Synthetic benchmarks cannot predict real-data duration")
    if {r["day"] for r in reports if r["dataset"] == "pod" and r["threads"] == threads} != {
        3,
        109,
        173,
    }:
        raise ValueError("Representative pod days 3, 109 and 173 are required")
    hours = [0.0, 0.0]
    details = {}
    for dataset, total in TOTALS.items():
        cases = [r for r in reports if r["dataset"] == dataset and r["threads"] == threads]
        if not cases:
            raise ValueError(f"Missing {threads}-thread benchmark for {dataset}")
        # Startup/validation overhead per partition cannot scale with source bytes.
        # Conservatively use full wall time per byte AND per-partition floor.
        rates = [r["wall_seconds"] / r["source_bytes"] for r in cases]
        overhead = [max(0, r["wall_seconds"] - r["elapsed_seconds"]) for r in cases]
        low = max(total * min(rates), COUNTS[dataset] * min(overhead))
        high = max(total * max(rates), COUNTS[dataset] * max(overhead)) * (1 + retry_fraction)
        details[dataset] = {"hours_scenario": [low / 3600, high / 3600], "pilot_cases": len(cases)}
        hours[0] += low / 3600
        hours[1] += high / 3600
    audit_details = {}
    if not audit_reports:
        raise ValueError("Completed final audit timings are required")
    for dataset in ("pod", "network"):
        times = [a["wall_seconds"] for a in audit_reports if a["dataset"] == dataset]
        expected_days = {3, 109, 173} if dataset == "pod" else {109}
        if {a["day"] for a in audit_reports if a["dataset"] == dataset} != expected_days:
            raise ValueError(f"Missing representative {dataset} audit timings")
        low = COUNTS[dataset] * min(times) / 3600
        high = COUNTS[dataset] * max(times) * (1 + retry_fraction) / 3600
        hours[0] += low
        hours[1] += high
        audit_details[dataset] = {"hours_scenario": [low, high]}
    return {
        "single_worker_hours_scenario": hours,
        "final_audits": audit_details,
        "by_dataset": details,
        "two_identical_workers_ideal_only_hours": [v / 2 for v in hours],
        "caveat": "Two-worker figures ignore startup, skew, coordination and shared bottlenecks. Benchmark concurrency before adopting.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--threads", type=int, default=6)
    args = parser.parse_args()
    result = json.loads(args.results.read_text())
    if result.get("status") != "complete":
        parser.error("Benchmark must finish successfully, including final audits")
    print(
        json.dumps(
            estimate(result["reports"], args.threads, audit_reports=result.get("audit_reports")),
            indent=2,
        )
    )
