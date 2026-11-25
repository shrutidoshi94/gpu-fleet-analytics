"""Offline product behavior, cohort math and snapshot integrity."""
from pathlib import Path
import tempfile,unittest,os
from unittest.mock import patch
import pandas as pd
from streamlit.testing.v1 import AppTest
from dashboard_data import hourly,scenario,activity_review,independent_checks,verify_bundle
from examples.make_dashboard_sample import make

ROOT=Path(__file__).resolve().parents[1]
class DashboardMath(unittest.TestCase):
 def test_concurrent_percentile_and_unknowns(self):
  rows=[]
  for hour,values in enumerate(((10.,0.),(0.,10.),(None,None))):
   for i,value in enumerate(values):
    rows.append(dict(day=0,hour=hour,gpu_spec_public='A10',inventory_gpu_slots=10,compatible_observed_gpu_requests=value,
      compatible_valid_request_rows=int(value is not None),inventory_server_hours=1,valid_capacity_server_hours=1,
      servers_with_pod_observations=int(value is not None),matched_pod_observations=1,valid_request_rows=int(value is not None),mismatched_server_hours=0,
      source_anomaly=False,duplicate_candidates_present=False))
  df=pd.DataFrame(rows);df.loc[df.hour.eq(2),'inventory_gpu_slots']=20;h=hourly(df)
  self.assertEqual(h.compatible_observed_gpu_requests.quantile(.95),10.)
  self.assertTrue(pd.isna(h.iloc[-1].compatible_observed_gpu_requests))
  r=scenario(df,.2,.2)
  self.assertEqual(r['modeled_slots'],15)
  self.assertEqual(r['capacity'],40.)
  self.assertEqual(r['baseline_hour'],2)
  self.assertEqual(scenario(df,0,0,baseline='Minimum observed capacity in window')['capacity'],20.)
  df.loc[0,'gpu_spec_public']='XPU-A'
  with self.assertRaisesRegex(ValueError,'one GPU cohort'):scenario(df,0,0)
 def test_weighted_measurements(self):
  a=pd.DataFrame([dict(day=0,hour=i,gpu_spec_public='A10',sm_source_unit_sum=s,sm_valid_observations=n,gpu_requesting_observations=n,observed_gpu_requests=1.,reported_gpu_hours=1.) for i,(s,n) in enumerate(((10.,1),(900.,9)))])
  review=activity_review(a,20,0)
  self.assertEqual(review.iloc[0]['Measured source-unit SM mean'],91.)
  self.assertEqual(review.iloc[0]['Pattern share'],.5)

class DashboardOffline(unittest.TestCase):
 def test_views_and_integrity_without_network(self):
  with tempfile.TemporaryDirectory() as folder:
   bundle=make(Path(folder)/'bundle')
   self.assertEqual(independent_checks(bundle)['daily_inventory_checks'],3)
   with patch.dict(os.environ,{'GOLD_BUNDLE_DIR':str(bundle),'AWS_PROFILE':'does-not-exist','AWS_EC2_METADATA_DISABLED':'true'}),patch('socket.create_connection',side_effect=AssertionError('Network forbidden')):
    a=AppTest.from_file(str(ROOT/'dashboard.py')).run(timeout=60)
    self.assertFalse(a.exception,str(a.exception))
    self.assertEqual([h.value for h in a.header],['Find the pressure','Review existing capacity','Test the requirement'])
    self.assertFalse(a.radio)
    self.assertTrue(any('Synthetic demo' in c.value for c in a.caption))
    a.selectbox(key='focus').select('XPU-A').run(timeout=60)
    self.assertTrue(a.metric[0].label.startswith('XPU-A'))
    self.assertTrue(any('Illustrative sizing for XPU-A' in c.value for c in a.caption))
    for detail in ('Workload mix','Network','Execution','AWS evidence','Quality'):
     a.selectbox(key='detail').select(detail).run(timeout=60)
     self.assertFalse(a.exception,(detail,str(a.exception)))
    a.checkbox(key='scenario_ack').check().run(timeout=60)
    self.assertTrue(any(m.label=='Modeled additional slots' for m in a.metric))
    a.selectbox(key='detail').select('Network').run(timeout=60)
    a.slider(key='days').set_range(0,5).run(timeout=60)
    self.assertTrue(any('no aligned network data' in i.value.lower() for i in a.info))
    a.slider(key='days').set_range(5,5).run(timeout=60)
    self.assertFalse(a.exception,str(a.exception))
    self.assertTrue(any(m.value=='Unknown' for m in a.metric))
   target=bundle/'capacity_hourly.parquet';changed=bytearray(target.read_bytes());changed[-1]^=1;target.write_bytes(changed)
   with self.assertRaisesRegex(ValueError,'integrity failed'):verify_bundle(bundle)

if __name__=='__main__':unittest.main()
