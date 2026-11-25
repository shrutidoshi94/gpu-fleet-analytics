"""Exercise real Spark, replacement semantics, and the offline Streamlit view."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from zipfile import ZipFile

import pyarrow.parquet as pq
from streamlit.testing.v1 import AppTest

PROJECT = Path(__file__).resolve().parents[1]


class SampleEndToEnd(unittest.TestCase):
    def test_sample_retry_and_offline_dashboard(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = os.environ.copy()
            env.pop('TRACE_BUCKET', None)
            env.pop('SPARK_HOME', None)
            env['SPARK_LOCAL_IP'] = '127.0.0.1'
            env['TRACE_DIR'] = str(root / 'sample')
            env['PYSPARK_PYTHON'] = sys.executable
            def run(*args, success=True):
                result = subprocess.run([sys.executable, *map(str, args)], cwd=root,
                                        env=env, capture_output=True, text=True)
                if success:
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return result
            run(PROJECT / 'examples/make_sample.py')
            run(PROJECT / 'pipelines/pipeline.py', 'network', 109)
            paths = [root / f'output/{layer}/network/day=109'
                     for layer in ('bronze', 'silver', 'gold')]
            summary = paths[2] / 'summary.json'
            original = summary.read_bytes()
            report = json.loads(original)
            self.assertEqual((report['dataset'], report['day']), ('network', 109))
            self.assertEqual(len(report['rows']), 24)
            for hour, row in enumerate(report['rows']):
                self.assertEqual((row['hour'], row['observations']), (hour, 2))
                self.assertTrue(math.isclose(row['mean_rx_gibps'], hour * (1/48 + 1/40)/2, abs_tol=1e-12))
                self.assertTrue(math.isclose(row['mean_tx_gibps'], hour * (1/72 + 1/60)/2, abs_tol=1e-12))
            for path in paths:
                (path / 'stale.txt').write_text('must be replaced')
            neighbor = root / 'output/gold/network/day=110/summary.json'
            neighbor.parent.mkdir(parents=True)
            neighbor.write_text('preserve other days')
            run(PROJECT / 'pipelines/pipeline.py', 'network', 109)
            self.assertEqual(summary.read_bytes(), original)
            self.assertEqual(neighbor.read_text(), 'preserve other days')
            for path in paths:
                self.assertFalse((path / 'stale.txt').exists())
            for path in paths[:2]:
                count = sum(pq.ParquetFile(p).metadata.num_rows for p in path.rglob('*.parquet'))
                self.assertEqual(count, 48)
            # Missing-hour input must fail without replacing a successful result.
            archive = root / 'sample/asi_opensource_network_hourly.zip'
            with ZipFile(archive) as source:
                entries = [(n, source.read(n)) for n in source.namelist() if '/hour=23/' not in n]
            with ZipFile(archive, 'w') as target:
                for name, data in entries:
                    target.writestr(name, data)
            failed = run(PROJECT / 'pipelines/pipeline.py', 'network', 109, success=False)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn('Expected 24 hours', failed.stderr)
            self.assertEqual(summary.read_bytes(), original)
            # A separate directory contains only the saved Gold JSON, no trace or AWS.
            offline = root / 'offline/output/gold/network/day=109'
            offline.mkdir(parents=True)
            (offline / 'summary.json').write_bytes(original)
            previous = Path.cwd()
            try:
                os.chdir(root / 'offline')
                app = AppTest.from_file(str(PROJECT / 'app/dashboard.py')).run(timeout=30)
                self.assertEqual(len(app.exception), 0, str(app.exception))
                self.assertEqual(len(app.dataframe[0].value), 24)
                self.assertIn('109', app.subheader[0].value)
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
