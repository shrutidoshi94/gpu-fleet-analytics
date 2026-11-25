"""Emit SSM bootstrap parameters for a pinned source archive; makes no AWS calls."""

import argparse
import json
import re
import shlex

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--region", default="us-east-2")
    a = p.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", a.sha):
        p.error("Expected a full Git SHA")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", a.bucket):
        p.error("Invalid bucket name")
    q = shlex.quote
    commands = [
        "set -euo pipefail",
        "mkdir -p /opt/gpu-fleet",
        f"aws s3 cp {q('s3://' + a.bucket + '/source/' + a.sha + '.tar.gz')} /tmp/gpu-source.tar.gz --only-show-errors",
        "tar -xzf /tmp/gpu-source.tar.gz -C /opt/gpu-fleet",
        f"TRACE_BUCKET={q(a.bucket)} AWS_DEFAULT_REGION={q(a.region)} bash /opt/gpu-fleet/infra/bootstrap-worker.sh",
    ]
    print(json.dumps({"commands": commands, "executionTimeout": ["3600"]}, indent=2))
