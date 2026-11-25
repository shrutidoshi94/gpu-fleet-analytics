import tempfile,time,sys,unittest
from pathlib import Path
from ops.silver_task import guarded
class SilverGuard(unittest.TestCase):
 def test_reject_disk_before_start(self):
  with tempfile.TemporaryDirectory() as d,open(Path(d)/'log','w') as log:
   marker=Path(d)/'started'
   with self.assertRaisesRegex(RuntimeError,'scratch'):
    guarded([sys.executable,'-c',f'open({str(marker)!r},"w").close()'],log,d,min_free=10**30)
   self.assertFalse(marker.exists())
 def test_timeout_and_failure(self):
  with tempfile.TemporaryDirectory() as d,open(Path(d)/'log','w') as log:
   began=time.monotonic()
   with self.assertRaises(TimeoutError):
    guarded([sys.executable,'-c','import time;time.sleep(30)'],log,d,min_free=0,max_seconds=.1,poll_seconds=.03)
   self.assertLess(time.monotonic()-began,5)
   with self.assertRaisesRegex(RuntimeError,'exited 7'):
    guarded([sys.executable,'-c','raise SystemExit(7)'],log,d,min_free=0,poll_seconds=.03)
