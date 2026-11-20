"""Resume must invalidate changed code, sources, settings, or output objects."""
from pathlib import Path
import tempfile
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from examples.make_silver_sample import make
from examples.benchmark_silver import reusable
from silver import CODE_SHA256, Store, source_inventory

class BenchmarkResume(unittest.TestCase):
    def test_resume_checks_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            make(folder)
            store=Store(folder)
            report={'dataset':'network','day':109,'driver_memory':'12g','code_sha256':CODE_SHA256,
                    'source_fingerprint':source_inventory(store,'network',109)[2],
                    'accepted':'silver_versions/network/day=109/test/accepted',
                    'quarantine':'silver_versions/network/day=109/test/quarantine'}
            for area in ('accepted','quarantine'):
                path=Path(store.uri(report[area]));path.mkdir(parents=True)
                pq.write_table(pa.table({'value':[1] if area=='accepted' else pa.array([],type=pa.int64())}),path/'part.parquet')
            report['output_files']={area:store.files(report[area]) for area in ('accepted','quarantine')}
            self.assertTrue(reusable(store,report,'12g'))
            self.assertFalse(reusable(store,report,'8g'))
            self.assertFalse(reusable(store,dict(report,code_sha256='old-code'),'12g'))
            marker=store.read('manifests/bronze/network.success.json')
            store.write('manifests/bronze/network.success.json',dict(marker,changed=True))
            self.assertFalse(reusable(store,report,'12g'))
            store.write('manifests/bronze/network.success.json',marker)
            Path(store.uri(report['accepted']+'/part.parquet')).write_bytes(b'truncated')
            self.assertFalse(reusable(store,report,'12g'))
