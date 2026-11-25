"""Real Spark, independently checked with Arrow; no AWS required."""
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[1] / 'pipelines'))
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, types as T
from silver import SCHEMAS, Store, run

class SilverEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('silver-tests').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def seed(self,root,dataset):
        schema = T.StructType([T.StructField(k,T._parse_datatype_string(v),True) for k,v in SCHEMAS[dataset].items()])
        base = {k:('id' if v=='string' else (False if v=='boolean' else (0.0 if v=='double' else 0))) for k,v in SCHEMAS[dataset].items()}
        rows=[]
        for i in range(4):
            row=dict(base,server_id=f's{i}')
            if 'pod_id' in row: row.update(pod_id=f'p{i}',workload_id=None,gpu_request=[0.0,0.5,None,2.0][i])
            if dataset=='pod': row.update(avg_gpu_sm_util=[None,0.0,float('nan'),10.0][i],avg_cpu_request_util=2.37,ready_delay_sec=-1,state_public='Unknown')
            rows.append(row)
        if dataset in ('server','network'):
            rows.append(dict(rows[0]))
            rows[1]['server_id']=None
        if dataset=='execution':
            self.spark.createDataFrame(rows,schema).write.parquet(str(root/f'bronze/{dataset}/all'))
        else:
            daily=[dict(row,hour=h) for h in range(24) for row in rows]
            self.spark.createDataFrame(daily,T.StructType(schema.fields+[T.StructField('hour',T.IntegerType(),False)])).write.partitionBy('hour').parquet(str(root/f'bronze/{dataset}/day=109'))
        store=Store(str(root))
        store.write(f'manifests/bronze/{dataset}.success.json',{'synthetic':True})
        files=store.files(f'bronze/{dataset}')
        store.write(f'manifests/bronze/{dataset}.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
        return store
    def rows(self,store,result,area):
        return [r for f in store.files(result[area]) for r in pq.ParquetFile(store.uri(f['path'])).read().to_pylist()]
    def test_full_contract(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            for dataset in ('server','pod','network','execution'):
                with self.subTest(dataset=dataset):
                    store=self.seed(root,dataset)
                    result=run(self.spark,store,dataset,109,output_partitions=1)
                    accepted=self.rows(store,result,'accepted')
                    rejected=self.rows(store,result,'quarantine')
                    self.assertEqual(len(accepted)+len(rejected),result['quality']['input_rows'])
                    self.assertTrue(set(SCHEMAS[dataset]) <= set(accepted[0]))
                    if dataset in ('pod','network'):
                        audit=result['quality']['inventory_join_audit']
                        self.assertEqual(audit['status'],'complete')
                        self.assertEqual(audit['matched_observations'],48)
                    if dataset=='pod':
                        self.assertEqual(len(accepted),96)
                        self.assertEqual(sum(r['gpu_request']==0 for r in accepted),24)
                        self.assertEqual(sum(r['is_gpu_requesting'] is None for r in accepted),24)
                        self.assertEqual(sum(r['has_sm_measurement'] for r in accepted),48)
                        self.assertEqual(result['quality']['non_finite_counts']['avg_gpu_sm_util']['nan'],24)
                        self.assertEqual(result['quality']['unknown_literal_counts']['state_public'],96)
                        self.assertEqual(result['quality']['boolean_counts']['ready_status']['false'],96)
                        self.assertTrue(all(r['avg_cpu_request_util']==2.37 and r['ready_delay_sec']==-1 for r in accepted))
                    if dataset in ('server','network'): self.assertEqual((len(accepted),len(rejected)),(48,72))
                    if dataset=='execution':
                        self.assertIsNone(result['day'])
                        self.assertNotIn('day',accepted[0])
                        self.assertNotIn('hour',accepted[0])
                    self.assertTrue(run(self.spark,store,dataset,109)['skipped'])
                    key=f'manifests/silver/{dataset}/'+('all' if dataset=='execution' else 'day=109')+'.current.json'
                    original=store.read(key)
                    write=store.write
                    def fail_commit(k,v):
                        if k.endswith('.current.json'): raise RuntimeError('injected commit failure')
                        write(k,v)
                    with patch.object(store,'write',side_effect=fail_commit):
                        with self.assertRaisesRegex(RuntimeError,'injected'):
                            run(self.spark,store,dataset,109,force=True,output_partitions=1)
                    self.assertEqual(store.read(key),original)
                    rerun=run(self.spark,store,dataset,109,force=True,output_partitions=1)
                    self.assertNotEqual(rerun['accepted'],result['accepted'])
                    self.assertEqual(rerun['quality'],result['quality'])
                    self.assertEqual(len(self.rows(store,rerun,'accepted')),len(accepted))
            import shutil
            shutil.rmtree(root/'bronze/network/day=109/hour=23')
            before=store.read('manifests/silver/network/day=109.current.json')
            with self.assertRaisesRegex(ValueError,'inventory|24 hours'): run(self.spark,store,'network',109)
            self.assertEqual(store.read('manifests/silver/network/day=109.current.json'),before)

    def test_schema_drift_keeps_previous_commit(self):
        import pyarrow as pa
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            store=self.seed(root,'network')
            run(self.spark,store,'network',109,output_partitions=1)
            key='manifests/silver/network/day=109.current.json'
            before=store.read(key)
            file=next((root/'bronze/network/day=109/hour=23').glob('*.parquet'))
            table=pq.ParquetFile(file).read()
            table=table.set_column(table.schema.get_field_index('rx_gibps_avg'),'rx_gibps_avg',pa.array(['bad']*len(table)))
            pq.write_table(table,file)
            file.with_name("."+file.name+".crc").unlink(missing_ok=True)
            files=store.files('bronze/network')
            store.write('manifests/bronze/network.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
            with self.assertRaisesRegex(ValueError,'schema drift'):
                run(self.spark,store,'network',109)
            self.assertEqual(store.read(key),before)

    def test_null_only_physical_column_uses_nullable_logical_type(self):
        import pyarrow as pa
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            store=self.seed(root,'server')
            for file in (root/'bronze/server').rglob('*.parquet'):
                table=pq.ParquetFile(file).read()
                table=table.set_column(table.schema.get_field_index('asw_id'),'asw_id',pa.nulls(len(table)))
                pq.write_table(table,file)
                file.with_name("."+file.name+".crc").unlink(missing_ok=True)
            files=store.files('bronze/server')
            store.write('manifests/bronze/server.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
            result=run(self.spark,store,'server',109,output_partitions=1)
            accepted=self.rows(store,result,'accepted')
            self.assertTrue(all(r['asw_id'] is None for r in accepted))
            self.assertEqual(result['quality']['null_counts']['asw_id'],120)
            self.assertEqual(len(accepted),48)
            # Mix one populated physical string file with null-only files.
            file=next((root/'bronze/server').rglob('*.parquet'))
            table=pq.ParquetFile(file).read()
            table=table.set_column(table.schema.get_field_index('asw_id'),'asw_id',pa.array(['known-switch']*len(table)))
            pq.write_table(table,file)
            file.with_name('.'+file.name+'.crc').unlink(missing_ok=True)
            files=store.files('bronze/server')
            store.write('manifests/bronze/server.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
            mixed=run(self.spark,store,'server',109,output_partitions=1)
            rows=self.rows(store,mixed,'accepted')
            self.assertEqual(mixed['quality']['null_counts']['asw_id'],120-len(table))
            self.assertEqual(len(rows),48)
            self.assertTrue(any(r['asw_id']=='known-switch' for r in rows))
            self.assertTrue(all(r['source_file'].endswith('.parquet') for r in rows))

    def test_lossless_numeric_widening_and_precision_rejection(self):
        import pyarrow as pa
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);store=self.seed(root,'network')
            file=next((root/'bronze/network/day=109/hour=0').glob('*.parquet'))
            table=pq.ParquetFile(file).read()
            table=table.set_column(table.schema.get_field_index('rx_gibps_avg'),'rx_gibps_avg',pa.array([1]*len(table),type=pa.int64()))
            table=table.set_column(table.schema.get_field_index('tx_gibps_avg'),'tx_gibps_avg',pa.array([0.25]*len(table),type=pa.float32()))
            def replace_source(value):
                pq.write_table(value,file);file.with_name('.'+file.name+'.crc').unlink(missing_ok=True)
                files=store.files('bronze/network')
                store.write('manifests/bronze/network.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
            replace_source(table)
            result=run(self.spark,store,'network',109,output_partitions=1)
            rows=[r for r in self.rows(store,result,'accepted') if r['hour']==0]
            self.assertTrue(rows)
            self.assertTrue(all(r['rx_gibps_avg']==1.0 and r['tx_gibps_avg']==0.25 for r in rows))
            previous=store.read('manifests/silver/network/day=109.current.json')
            unsafe=table.set_column(table.schema.get_field_index('rx_gibps_avg'),'rx_gibps_avg',pa.array([2**53+1]*len(table),type=pa.int64()))
            replace_source(unsafe)
            with self.assertRaisesRegex(ValueError,'exact integer range'):
                run(self.spark,store,'network',109)
            self.assertEqual(store.read('manifests/silver/network/day=109.current.json'),previous)

    def test_source_warning_is_committed_and_partition_scoped(self):
        from silver import source_inventory
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);store=self.seed(root,'network')
            key=store.files('bronze/network/day=109')[0]['path']
            warning={'dataset':'network','source_key':key,'code':'synthetic_source_review','rows':5}
            store.write('manifests/bronze/quality-observations.json',{'observations':[warning]})
            before=source_inventory(store,'network',109)[2]
            result=run(self.spark,store,'network',109,output_partitions=1)
            self.assertEqual(result['quality']['source_observations'],[warning])
            neighbor=dict(warning,source_key='bronze/network/day=110/hour=00/other.parquet')
            store.write('manifests/bronze/quality-observations.json',{'observations':[warning,neighbor]})
            self.assertEqual(source_inventory(store,'network',109)[2],before)
            self.assertTrue(run(self.spark,store,'network',109)['skipped'])
