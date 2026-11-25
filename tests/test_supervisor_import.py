"""Worker modules import from the installed package outside the checkout."""

import pathlib
import subprocess
import sys
import tempfile
import unittest


class SupervisorImports(unittest.TestCase):
    def test_worker_package_imports(self):
        root = pathlib.Path(__file__).resolve().parents[1]
        probe = (
            "import sys; from pathlib import Path; "
            "import ops.silver_supervisor, ops.gold_task, pipelines.silver; "
            "assert Path(pipelines.silver.__file__).resolve() == Path(sys.argv[1]).resolve()"
        )
        with tempfile.TemporaryDirectory() as cwd:
            subprocess.run(
                [sys.executable, "-I", "-c", probe, str(root / "pipelines/silver.py")],
                cwd=cwd,
                check=True,
            )
