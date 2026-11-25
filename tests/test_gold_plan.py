"""Gold runs cover arbitrary day subsets and reject stale pilot timing."""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ops.gold_plan import pilot_days, remaining_days


class GoldPlan(unittest.TestCase):
    def test_every_requested_day_runs_once(self):
        for days in ([0], [109], [0, 1], [3, 5], [0, 1, 3, 109], list(range(185))):
            with self.subTest(days=days):
                pilot = pilot_days(days)
                rest = remaining_days(days)
                self.assertTrue(pilot)
                self.assertFalse(set(pilot) & set(rest))
                self.assertEqual(sorted(pilot + rest), sorted(days))
                self.assertLessEqual(len(pilot), 4)
        self.assertEqual(pilot_days([0]), [0])
        self.assertEqual(remaining_days([109]), [])

    def test_pilot_requires_selected_days_and_matching_plan(self):
        from ops import gold_task

        with tempfile.TemporaryDirectory() as tmp:
            store = Mock()
            store.read.return_value = {"days": [0], "identity": "current"}
            path = Path(tmp) / "day-0.json"
            value = {
                "release": "ci",
                "day": 0,
                "status": "success",
                "plan_identity": "old",
                "elapsed_seconds": 5,
            }
            path.write_text(json.dumps(value))
            argv = ["gold_task", "pilot-check", "--root", tmp, "--release", "ci", "--days", "0"]
            with (
                patch.object(gold_task, "Store", return_value=store),
                patch.dict(os.environ, {"TRACE_GOLD_LOG_DIR": tmp}),
                patch.object(sys, "argv", argv),
            ):
                with self.assertRaisesRegex(ValueError, "pinned plan"):
                    gold_task.main()
                value["plan_identity"] = "current"
                path.write_text(json.dumps(value))
                gold_task.main()
            self.assertEqual(store.write.call_args.args[1]["pilot_days"], [0])

    @unittest.skipUnless(
        importlib.util.find_spec("airflow"), "Airflow graph checked in pipeline CI"
    )
    def test_graph_allows_empty_remaining_map(self):
        from airflow.utils.trigger_rule import TriggerRule

        from dags.gpu_gold import dag, pilot

        context = dag.get_task("global_context")
        self.assertEqual(context.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        self.assertTrue({"pilot", "gold_day"} <= context.upstream_task_ids)
        with patch("dags.gpu_gold.command") as command:
            pilot.function("ci", {"days": [0]})
            self.assertEqual(command.call_args_list[0].args, ("day", "ci", ["--day", "0"]))
            self.assertEqual(
                command.call_args_list[-1].args, ("pilot-check", "ci", ["--days", "0"])
            )
