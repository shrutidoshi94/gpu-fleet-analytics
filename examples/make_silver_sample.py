"""Generate all four full-width synthetic Bronze tables and source inventories."""
import argparse
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from silver import SCHEMAS, Store

def make(root):
    store=Store(str(Path(root).resolve()))
    types={'string':pa.string(),'bigint':pa.int64(),'double':pa.float64(),'boolean':pa.bool_()}
    for dataset,columns in SCHEMAS.items():
        rows=[]
        for i in range(3):
            row={k:('synthetic' if t=='string' else (False if t=='boolean' else (0.0 if t=='double' else 0))) for k,t in columns.items()}
            row['server_id']=f'synthetic-server-{i}'
            if 'pod_id' in row: row.update(pod_id=f'synthetic-pod-{i}',workload_id=None,gpu_request=[0.0,0.5,2.0][i])
            if dataset=='server': row.update(gpu_count=4,cpu_capacity_cores=32)
            if dataset=='pod': row.update(avg_gpu_sm_util=[None,0.0,42.0][i],avg_cpu_request_util=2.37,ready_delay_sec=-1)
            rows.append(row)
        table=pa.Table.from_pylist(rows,schema=pa.schema([pa.field(k,types[t]) for k,t in columns.items()]))
        paths=[f'bronze/{dataset}/all/part-000.parquet'] if dataset=='execution' else [f'bronze/{dataset}/day=109/hour={h:02}/part-000.parquet' for h in range(24)]
        for relative in paths:
            path=Path(store.uri(relative)); path.parent.mkdir(parents=True,exist_ok=True)
            pq.write_table(table,path)
        files=store.files(f'bronze/{dataset}')
        store.write(f'manifests/bronze/{dataset}.json',{'files':[{'key':f['path'],'bytes':f['bytes']} for f in files]})
        store.write(f'manifests/bronze/{dataset}.success.json',{'dataset':dataset,'synthetic':True,'file_count':len(files)})

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root')
    make(parser.parse_args().root)
