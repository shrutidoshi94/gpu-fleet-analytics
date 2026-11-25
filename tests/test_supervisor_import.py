"""The deployed ops entry point must resolve project modules outside the checkout cwd."""
import pathlib
import subprocess
import sys
import tempfile
import unittest


class SupervisorImports(unittest.TestCase):
    def test_project_import_from_isolated_interpreter(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        probe="import runpy,sys,importlib.util; runpy.run_path(sys.argv[1],run_name='probe'); spec=importlib.util.find_spec('silver'); assert spec and spec.origin==sys.argv[2], spec"
        with tempfile.TemporaryDirectory() as cwd:
            subprocess.run([sys.executable,'-I','-c',probe,str(root/'ops/silver_supervisor.py'),str(root/'silver.py')],cwd=cwd,check=True)
