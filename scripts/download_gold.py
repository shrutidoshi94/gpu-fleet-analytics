"""Download and verify a Gold release once; the dashboard itself needs no AWS."""

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from app.dashboard_data import independent_checks

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--bucket", required=True)
p.add_argument("--release", default="gold_20261120")
p.add_argument("--profile", default="gpu-fleet")
p.add_argument("--region", default="us-east-2")
p.add_argument("--destination", type=Path)
a = p.parse_args()
if not re.fullmatch(r"[A-Za-z0-9_-]+", a.release):
    p.error("Invalid release name")
destination = (
    a.destination or Path(__file__).resolve().parents[1] / "data/gold" / a.release
).resolve()
if destination.exists():
    print(json.dumps(independent_checks(destination), indent=2))
    print("Existing bundle verified; no download needed.")
    sys.exit(0)
destination.parent.mkdir(parents=True, exist_ok=True)
stage = Path(tempfile.mkdtemp(prefix="gold-download-", dir=destination.parent))
try:
    subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            f"s3://{a.bucket}/gold_exports/{a.release}/",
            str(stage),
            "--profile",
            a.profile,
            "--region",
            a.region,
            "--only-show-errors",
            "--no-progress",
        ],
        check=True,
    )
    report = independent_checks(stage)
    (stage / "LOCAL_VERIFICATION.json").write_text(json.dumps(report, indent=2) + "\n")
    stage.rename(destination)
    print(json.dumps(report, indent=2))
    print("Saved verified bundle:", destination)
except BaseException:
    shutil.rmtree(stage, ignore_errors=True)
    raise
