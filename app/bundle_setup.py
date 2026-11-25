"""Install a pinned public Gold asset once; subsequent reads are local."""

import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

from app.dashboard_data import independent_checks


def install_bundle(spec, destination):
    destination = Path(destination)
    if destination.exists():
        report = independent_checks(destination)
        if report["release"] != spec["release"]:
            raise ValueError("Existing bundle release differs from the pinned release")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".gold-setup-", dir=destination.parent))
    try:
        archive = stage / "download.zip"
        with urlopen(spec["url"], timeout=120) as response, archive.open("wb") as output:
            digest = hashlib.sha256()
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > spec["bytes"]:
                    raise ValueError("Gold download exceeds pinned size")
                digest.update(chunk)
                output.write(chunk)
        if size != spec["bytes"] or digest.hexdigest() != spec["sha256"]:
            raise ValueError("Gold archive checksum or size differs from the pinned release")
        extracted = stage / "bundle"
        extracted.mkdir()
        with ZipFile(archive) as source:
            members = source.infolist()
            if any(Path(m.filename).name != m.filename or m.is_dir() for m in members):
                raise ValueError("Unsafe Gold archive member")
            if len({m.filename for m in members}) != len(members):
                raise ValueError("Duplicate Gold archive member")
            if sum(m.file_size for m in members) != spec["unpacked_bytes"]:
                raise ValueError("Gold archive expanded size differs")
            source.extractall(extracted)
        manifest = json.loads((extracted / "bundle.json").read_text())
        if {m.filename for m in members} != {"bundle.json", *manifest["files"]}:
            raise ValueError("Unexpected Gold archive contents")
        report = independent_checks(extracted)
        if report["release"] != spec["release"]:
            raise ValueError("Gold release differs from pinned release")
        extracted.rename(destination)
        return destination
    finally:
        shutil.rmtree(stage, ignore_errors=True)
