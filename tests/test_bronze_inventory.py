import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from pipelines.ingest_bronze import ForwardReader, inventory


class BronzeInventoryTest(unittest.TestCase):
    def test_forward_reader_prevents_zip_prescan(self):
        import io

        reader = ForwardReader(io.BytesIO(b"original payload"))
        self.assertFalse(hasattr(reader, "seek"))
        self.assertEqual(reader.read(9), b"original ")
        self.assertEqual(reader.read(), b"payload")

    def test_network_full_coverage_and_missing_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "network.zip"
            names = [
                f"asi_opensource_network_hourly/day={day}/hour={hour:02}/part-000.parquet"
                for day in range(109, 116)
                for hour in range(24)
            ]
            with ZipFile(path, "w") as archive:
                for name in names:
                    archive.writestr(name, b"original bytes")
            entries = inventory(path, "network")
            self.assertEqual(len(entries), 168)
            self.assertEqual(entries[0]["key"], "bronze/network/day=109/hour=00/part-000.parquet")
            with ZipFile(path, "w") as archive:
                for name in names[:-1]:
                    archive.writestr(name, b"original bytes")
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                inventory(path, "network")

    def test_execution_layout_and_unsafe_member(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "execution.zip"
            with ZipFile(path, "w") as archive:
                archive.writestr("asi_opensource_job_execution_summary/part-000.parquet", b"data")
            self.assertEqual(
                inventory(path, "execution")[0]["key"], "bronze/execution/all/part-000.parquet"
            )
            with ZipFile(path, "a") as archive:
                archive.writestr("../outside.parquet", b"data")
            with self.assertRaisesRegex(ValueError, "Unexpected"):
                inventory(path, "execution")
