"""Tiny explicitly synthetic, complete Gold bundle for offline UI tests. No Spark/AWS."""

import hashlib
import json
from pathlib import Path

import pandas as pd


def make(folder):
    folder = Path(folder)
    if folder.exists() and any(folder.iterdir()):
        raise ValueError("Use a new empty synthetic fixture directory")
    folder.mkdir(parents=True, exist_ok=True)
    tables = {
        k: []
        for k in (
            "capacity_hourly",
            "activity_evidence_hourly",
            "workload_mix_hourly",
            "placement_daily",
            "planning_quality",
            "network_context_hourly",
            "execution_context_global",
        )
    }
    for day in (0, 5, 109):
        for hour in range(24):
            for model in ("A10", "XPU-A"):
                base = dict(
                    day=day, hour=hour, gpu_spec_public=model, cluster_id="synthetic-cluster"
                )
                tables["capacity_hourly"].append(
                    dict(
                        base,
                        inventory_gpu_slots=8,
                        valid_capacity_server_hours=2,
                        inventory_server_hours=2,
                        servers_with_pod_observations=1,
                        matched_pod_observations=2,
                        valid_request_rows=2,
                        matched_observed_gpu_requests=4.0,
                        compatible_observed_gpu_requests=4.0,
                        compatible_valid_request_rows=2,
                        mismatched_server_hours=0,
                        over_requested_server_hours=0,
                        headroom_evidenced_server_hours=1,
                        min_observed_headroom=0.0,
                        max_observed_headroom=4.0,
                        partial_request_to_inventory_ratio=0.5,
                        source_anomaly=day == 5 and hour == 6,
                        duplicate_candidates_present=day == 5,
                        decision_ready=False,
                        eligibility_reason="synthetic",
                    )
                )
                tables["activity_evidence_hourly"].append(
                    dict(
                        base,
                        inventory_match=True,
                        cohort_match=True,
                        gpu_requesting_observations=2,
                        observed_gpu_requests=4.0,
                        sm_source_unit_sum=20.0,
                        sm_valid_observations=1,
                        gpu_memory_gib_sum=2.0,
                        gpu_memory_valid_observations=1,
                        cpu_request_util_sum=1.0,
                        cpu_util_valid_observations=1,
                        memory_util_sum=1.0,
                        memory_util_valid_observations=1,
                        reported_gpu_hours=1.0,
                    )
                )
                for dimension in (
                    "priority_class",
                    "job_type_public",
                    "model_type_public",
                    "state_public",
                    "is_genai_request",
                ):
                    tables["workload_mix_hourly"].append(
                        dict(
                            base,
                            inventory_match=True,
                            cohort_match=True,
                            dimension=dimension,
                            value="synthetic",
                            observations=2,
                            observed_gpu_requests=4.0,
                            valid_request_observations=2,
                        )
                    )
                tables["planning_quality"].append(
                    dict(
                        base,
                        inventory_match=True,
                        cohort_match=True,
                        observations=2,
                        valid_request_observations=2,
                    )
                )
                if day == 109:
                    tables["network_context_hourly"].append(
                        dict(
                            base,
                            observations=1,
                            rx_gibps_sum=0.1,
                            rx_valid_observations=1,
                            tx_gibps_sum=0.2,
                            tx_valid_observations=1,
                        )
                    )
        for model in ("A10", "XPU-A"):
            for b in ("unknown", ">4"):
                tables["placement_daily"].append(
                    dict(
                        day=day,
                        gpu_spec_public=model,
                        cluster_id="synthetic-cluster",
                        headroom_bin=b,
                        count=24,
                    )
                )
    for model in ("A10", "XPU-A"):
        tables["execution_context_global"].append(
            dict(
                gpu_spec_public=model,
                priority_class="synthetic",
                job_type_public="synthetic",
                observed_execution_records=3,
                observed_span_hours_sum=6.0,
                valid_span_records=3,
                schedule_delay_sec_sum=2.0,
                valid_schedule_delay_records=2,
                ready_delay_sec_sum=2.0,
                valid_ready_delay_records=2,
                ready_true_records=1,
                ready_false_records=1,
                ready_unknown_records=1,
            )
        )
    m = {
        "release": "synthetic_dashboard",
        "synthetic": True,
        "relative_days": [0, 5, 109],
        "plan_identity": "synthetic",
        "code_sha256": "synthetic",
        "tables": {},
        "files": {},
    }
    for name, rows in tables.items():
        df = pd.DataFrame(rows)
        df.to_parquet(folder / (name + ".parquet"), compression="zstd", index=False)
        m["tables"][name] = {"rows": len(df), "columns": list(df.columns)}
    (folder / "METRICS.md").write_text("Synthetic UI fixture; not the AWS trace.")
    (folder / "lineage.json").write_text(
        json.dumps(
            {
                "release": {
                    "units": {
                        f"day={d}": {
                            "quality": {"inventory_gpu_slot_hours": 384, "source_accepted_rows": 96}
                        }
                        for d in (0, 5, 109)
                    }
                }
            }
        )
    )
    for p in folder.iterdir():
        m["files"][p.name] = {
            "bytes": p.stat().st_size,
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
    m["total_bytes"] = sum(x["bytes"] for x in m["files"].values())
    (folder / "bundle.json").write_text(json.dumps(m, indent=2))
    return folder


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("destination")
    make(p.parse_args().destination)
