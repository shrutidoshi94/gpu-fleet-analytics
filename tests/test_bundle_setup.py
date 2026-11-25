"""Release installation verifies data and reuses local files without a second download."""
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile, ZIP_DEFLATED
from examples.make_dashboard_sample import make
from app.bundle_setup import install_bundle

class BundleSetup(unittest.TestCase):
    def test_verified_install_reuse_and_corrupt_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=make(root/'fixture'); blob=io.BytesIO()
            with ZipFile(blob,'w',ZIP_DEFLATED) as z:
                for path in source.iterdir(): z.write(path,path.name)
            payload=blob.getvalue()
            spec=dict(release='synthetic_dashboard',url='https://example.invalid/gold.zip',
                      bytes=len(payload),sha256=hashlib.sha256(payload).hexdigest(),
                      unpacked_bytes=sum(p.stat().st_size for p in source.iterdir()))
            with patch('app.bundle_setup.urlopen',return_value=io.BytesIO(payload)) as download:
                installed=install_bundle(spec,root/'installed')
                self.assertTrue((installed/'bundle.json').exists())
                install_bundle(spec,installed)
                self.assertEqual(download.call_count,1)
            bad=bytes([payload[0]^1])+payload[1:]
            with patch('app.bundle_setup.urlopen',return_value=io.BytesIO(bad)):
                with self.assertRaisesRegex(ValueError,'checksum'):
                    install_bundle(spec,root/'corrupt')
            self.assertFalse((root/'corrupt').exists())
            self.assertFalse(list(root.glob('.gold-setup-*')))
