"""Verify a downloaded production Gold bundle without AWS or Spark."""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.dashboard_data import independent_checks
p=argparse.ArgumentParser();p.add_argument('folder');p.add_argument('--report')
a=p.parse_args();report=independent_checks(a.folder)
if a.report:Path(a.report).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
