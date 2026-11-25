"""Package a verified local bundle for a GitHub Release (no AWS calls)."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from zipfile import ZipFile, ZIP_DEFLATED
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.dashboard_data import independent_checks

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('folder', type=Path)
    p.add_argument('archive', type=Path)
    p.add_argument('--url', required=True)
    args = p.parse_args()
    report = independent_checks(args.folder)
    manifest = json.loads((args.folder / 'bundle.json').read_text())
    names = sorted({'bundle.json', *manifest['files']})
    with ZipFile(args.archive, 'w', ZIP_DEFLATED) as archive:
        for name in names:
            archive.write(args.folder / name, name)
    print(json.dumps({'release': report['release'], 'url': args.url,
        'bytes': args.archive.stat().st_size,
        'sha256': hashlib.sha256(args.archive.read_bytes()).hexdigest(),
        'unpacked_bytes': sum((args.folder / n).stat().st_size for n in names)}, indent=2))
