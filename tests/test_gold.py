"""Gold joins, quality semantics and atomic publication on real local Spark."""
import json
from pathlib import Path
import tempfile
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from silver import Store, SCHEMAS, CODE_SHA256
from gold import pin, daily, context, publish
from ops.export_gold import export


class GoldContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('gold-contract').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self, folder):
        store=Store(folder)
        types={'string':pa.string(),'bigint':pa.int64(),'double':pa.float64(),'boolean':pa.bool_()}
        for ds, columns in SCHEMAS.items():
            base={k:('cohort' if t=='string' else False if t=='boolean' else 0.0 if t=='double' else 0) for k,t in columns.items()}
            if ds=='server':
                rows=[dict(base,server_id='s1',gpu_count=4),dict(base,server_id='s2',gpu_count=4),dict(base,server_id='unobserved',gpu_count=8),dict(base,server_id='unknown-cohort',gpu_spec_public='no-telemetry',gpu_count=2)]
            elif ds=='pod':
                rows=[dict(base,server_id='s1',pod_id='p1',gpu_request=1.0,avg_gpu_sm_util=0.0),
                      dict(base,server_id='s1',pod_id='p2',gpu_request=0.5,avg_gpu_sm_util=200.0),
                      dict(base,server_id='s2',pod_id='p3',gpu_request=float('nan'),avg_gpu_sm_util=None),
                      dict(base,server_id='missing',pod_id='p4',gpu_request=3.0)]
            else:
                rows=[dict(base,server_id='s1')]
            part='all' if ds=='execution' else 'day=109'
            if ds!='execution': rows=[dict(row,day=109,hour=h) for h in range(24) for row in rows]
            path=Path(folder)/f'silver_versions/{ds}/{part}/accepted';path.mkdir(parents=True)
            schema=pa.schema([pa.field(k,types[t]) for k,t in columns.items()]+([] if ds=='execution' else [pa.field('day',pa.int32()),pa.field('hour',pa.int32())]))
            pq.write_table(pa.Table.from_pylist(rows,schema=schema),path/'part.parquet')
            m={'synthetic_source':True,'code_sha256':CODE_SHA256,'accepted':str(path.relative_to(folder)),
               'quality':{'input_rows':len(rows),'accepted_rows':len(rows),'quarantined_rows':0,
                          'candidate_key_audit':{'duplicate_groups':0},'source_observations':([{'source_key':'bronze/pod/day=109/hour=06/part.parquet','code':'test_anomaly'}] if ds=='pod' else [])}}
            store.write(f'manifests/silver/{ds}/{part}.current.json',m)
        return store

    def test_capacity_unknown_and_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            store=self.fixture(folder);plan=pin(store,'test',[109],True)
            result=daily(self.spark,store,plan,109)
            table=self.spark.read.parquet(store.uri(result['tables']['capacity_hourly']['path']))
            first=table.filter("gpu_spec_public='cohort'").orderBy('hour').first()
            self.assertEqual(table.count(),48)
            unknown=table.filter("gpu_spec_public='no-telemetry'").first()
            self.assertIsNone(unknown.partial_request_to_inventory_ratio)
            self.assertEqual(unknown.servers_with_pod_observations,0)
            self.assertEqual(first.inventory_gpu_slots,16)
            self.assertEqual(first.matched_observed_gpu_requests,1.5)
            self.assertEqual(first.servers_with_pod_observations,2)
            self.assertEqual(first.headroom_evidenced_server_hours,1)
            self.assertFalse(first.decision_ready)
            self.assertTrue(table.filter('hour=6').first().source_anomaly)
            self.assertFalse(first.source_anomaly)
            activity=self.spark.read.parquet(store.uri(result['tables']['activity_evidence_hourly']['path']))
            measured=activity.filter('hour=0 AND inventory_match=true').first()
            self.assertEqual(measured.sm_source_unit_sum,200)
            self.assertEqual(measured.sm_valid_observations,2)
            with self.assertRaisesRegex(ValueError,'Incomplete'):
                publish(store,plan)
            context(self.spark,store,plan)
            release=publish(store,plan)
            self.assertEqual(set(release['units']),{'day=109','global'})
            bundle=export(store,'test',Path(folder)/'bundle')
            self.assertEqual(bundle['tables']['capacity_hourly']['rows'],48)
            self.assertEqual(export(store,'test',Path(folder)/'bundle'),bundle)
            self.assertEqual(bundle['execution_scope'],'global')
            self.assertTrue((Path(folder)/'bundle/bundle.json').exists())
            prior=store.read('manifests/gold/test/day=109.current.json')
            # Drift must fail without replacing a good Gold pointer/release.
            key='manifests/silver/pod/day=109.current.json';m=store.read(key);m['changed']=True;store.write(key,m)
            with self.assertRaisesRegex(ValueError,'Pinned Silver changed'):
                publish(store,plan)
            self.assertEqual(store.read('manifests/gold/test/day=109.current.json'),prior)

    def test_production_gate_cannot_be_bypassed(self):
        from unittest.mock import Mock
        store=Mock(remote=True)
        store.read.return_value={'verified_complete':False,'dag_state':'running'}
        with self.assertRaisesRegex(ValueError,'Verified complete'):
            pin(store,'test',[109],False)
        store.read.return_value={'verified_complete':True,'dag_state':'success'}
        with self.assertRaisesRegex(ValueError,'Synthetic bypass'):
            pin(store,'test',[109],True)

if __name__=='__main__':unittest.main()
