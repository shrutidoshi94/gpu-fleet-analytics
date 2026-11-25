"""Offline product behavior, cohort math and snapshot integrity."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.dashboard_data import (
    activity_summary,
    hourly,
    independent_checks,
    scenario,
    screening_summary,
    verify_bundle,
)
from examples.make_dashboard_sample import make

ROOT = Path(__file__).resolve().parents[1]


class DashboardMath(unittest.TestCase):
    def test_concurrent_percentile_and_unknowns(self):
        rows = []
        for hour, values in enumerate(((10.0, 0.0), (0.0, 10.0), (None, None))):
            for i, value in enumerate(values):
                rows.append(
                    dict(
                        day=0,
                        hour=hour,
                        gpu_spec_public="A10",
                        inventory_gpu_slots=10,
                        compatible_observed_gpu_requests=value,
                        compatible_valid_request_rows=int(value is not None),
                        inventory_server_hours=1,
                        valid_capacity_server_hours=1,
                        servers_with_pod_observations=int(value is not None),
                        matched_pod_observations=1,
                        valid_request_rows=int(value is not None),
                        mismatched_server_hours=0,
                        source_anomaly=False,
                        duplicate_candidates_present=False,
                    )
                )
        df = pd.DataFrame(rows)
        df.loc[df.hour.eq(2), "inventory_gpu_slots"] = 20
        h = hourly(df)
        self.assertEqual(h.compatible_observed_gpu_requests.quantile(0.95), 10.0)
        self.assertTrue(pd.isna(h.iloc[-1].compatible_observed_gpu_requests))
        r = scenario(df, 0.2, 0.2)
        self.assertEqual(r["modeled_slots"], 15)
        self.assertEqual(r["capacity"], 40.0)
        self.assertEqual(r["baseline_hour"], 2)
        self.assertEqual(
            scenario(df, 0, 0, baseline="Minimum observed capacity in window")["capacity"], 20.0
        )
        df.loc[0, "gpu_spec_public"] = "XPU-A"
        with self.assertRaisesRegex(ValueError, "one GPU cohort"):
            scenario(df, 0, 0)

    def test_weighted_measurements(self):
        a = pd.DataFrame(
            [
                dict(
                    day=0,
                    hour=i,
                    gpu_spec_public="A10",
                    sm_source_unit_sum=s,
                    sm_valid_observations=n,
                    gpu_requesting_observations=n,
                    observed_gpu_requests=1.0,
                    reported_gpu_hours=1.0,
                )
                for i, (s, n) in enumerate(((10.0, 1), (900.0, 9)))
            ]
        )
        review = activity_summary(a)
        self.assertEqual(review["mean"], 91.0)
        self.assertEqual(review["coverage"], 1.0)

    def test_screening_comparison_uses_same_inventory(self):
        from app.dashboard_data import read_table, select

        with tempfile.TemporaryDirectory() as tmp:
            source = make(Path(tmp) / "bundle")
            frame = select(read_table(source, "capacity_hourly"), (0, 109), "A10")
            comparison = screening_summary(frame)
            self.assertEqual(comparison["Hours with requests"].tolist(), [72, 48])
            flagged = frame.loc[frame.day.eq(5)].copy()
            empty = screening_summary(flagged)
            self.assertEqual(empty.iloc[1]["Hours with requests"], 0)
            self.assertTrue(pd.isna(empty.iloc[1]["p95 requests / inventory"]))


class DashboardOffline(unittest.TestCase):
    def test_views_and_integrity_without_network(self):
        with tempfile.TemporaryDirectory() as folder:
            bundle = make(Path(folder) / "bundle")
            self.assertEqual(independent_checks(bundle)["daily_inventory_checks"], 3)
            with (
                patch.dict(
                    os.environ,
                    {
                        "GOLD_BUNDLE_DIR": str(bundle),
                        "AWS_PROFILE": "does-not-exist",
                        "AWS_EC2_METADATA_DISABLED": "true",
                    },
                ),
                patch("socket.create_connection", side_effect=AssertionError("Network forbidden")),
            ):
                a = AppTest.from_file(str(ROOT / "app/dashboard.py")).run(timeout=60)
                self.assertFalse(a.exception, str(a.exception))
                self.assertEqual(
                    [h.value for h in a.header],
                    [
                        "Where is demand highest?",
                        "Could existing GPUs work harder?",
                        "What would growth require?",
                    ],
                )
                self.assertEqual(
                    [t.label for t in a.tabs],
                    ["1 · Demand pressure", "2 · Existing GPUs", "3 · Plan for growth"],
                )
                self.assertFalse(any(w.key == "focus" for w in a.selectbox))
                self.assertTrue(any("Synthetic demo" in c.value for c in a.caption))
                a.session_state["focus"] = "XPU-A"
                a.run(timeout=60)
                self.assertTrue(a.metric[0].label.startswith("XPU-A"))
                self.assertTrue(any("Illustrative sizing for XPU-A" in c.value for c in a.caption))
                for detail in ("Workload mix", "Network", "Execution", "AWS evidence", "Quality"):
                    a.radio(key="detail").set_value(detail).run(timeout=60)
                    self.assertFalse(a.exception, (detail, str(a.exception)))
                a.checkbox(key="scenario_ack").check().run(timeout=60)
                self.assertTrue(any(m.label == "Modeled additional slots" for m in a.metric))
                a.radio(key="detail").set_value("Network").run(timeout=60)
                a.slider(key="days").set_range(0, 5).run(timeout=60)
                self.assertTrue(any("no aligned network data" in i.value.lower() for i in a.info))
                a.slider(key="days").set_range(5, 5).run(timeout=60)
                self.assertFalse(a.exception, str(a.exception))
                self.assertTrue(any(m.value == "Unknown" for m in a.metric))
            target = bundle / "capacity_hourly.parquet"
            changed = bytearray(target.read_bytes())
            changed[-1] ^= 1
            target.write_bytes(changed)
            with self.assertRaisesRegex(ValueError, "integrity failed"):
                verify_bundle(bundle)


if __name__ == "__main__":
    unittest.main()
