"""Whole-trace footer/schema preflight. Does not claim row-level semantic validation."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import json,math
from pathlib import Path
import sys,time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from silver import Store,SCHEMAS,ARROW_TYPES,compatible_schema


def inspect(store,dataset,item):
    parquet=store.footer(item['path']);schema=parquet.schema_arrow;metadata=parquet.metadata
    actual={f.name:ARROW_TYPES.get(str(f.type),str(f.type)) for f in schema}
    columns={}
    for index,field in enumerate(schema):
        known_null=0;missing=0;extrema=[];unsafe=0;range_unknown=0
        for group in range(metadata.num_row_groups):
            rg=metadata.row_group(group);stats=rg.column(index).statistics
            if actual[field.name]=='null':known_null+=rg.num_rows;continue
            if stats is None:missing+=1
            elif stats.null_count is not None:known_null+=stats.null_count
            if stats is not None and stats.has_min_max:
                for value in (stats.min,stats.max):
                    if isinstance(value,float) and not math.isfinite(value):continue
                    extrema.append(value)
                if SCHEMAS[dataset].get(field.name)=='double' and actual[field.name]=='bigint':
                    if stats.min<-(2**53) or stats.max>2**53:unsafe+=1
            elif SCHEMAS[dataset].get(field.name)=='double' and actual[field.name]=='bigint':
                if stats is None or stats.null_count!=rg.num_rows:range_unknown+=1
        columns[field.name]={'physical_type':actual[field.name],'known_null_rows':known_null,'row_groups_missing_statistics':missing,
                             'min':min(extrema) if extrema else None,'max':max(extrema) if extrema else None,
                             'unsafe_integer_range_groups':unsafe,'integer_range_requires_row_check_groups':range_unknown}
    return {'key':item['path'],'rows':metadata.num_rows,'row_groups':metadata.num_row_groups,
            'schema':actual,'compatible':compatible_schema(dataset,actual),'columns':columns}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();store=Store(args.root);started=time.monotonic()
    if store.remote:
        import pyarrow.fs as fs
        store.arrow_fs=fs.S3FileSystem(region='us-east-2')
    report={'method':'All original Bronze Parquet footers, not a sample; statistics are metadata only',
            'limits':['NaN/Infinity counts, duplicates, conditional rules and joins still require row-level Spark checks',
                      'Missing footer statistics are unknown, not evidence of zero nulls/anomalies'],
            'tables':{},'errors':[],'observations':[]}
    for dataset in SCHEMAS:
        marker=store.read(f'manifests/bronze/{dataset}.success.json');inventory=store.read(f'manifests/bronze/{dataset}.json')
        if not marker or not inventory:raise RuntimeError('Verified Bronze evidence missing')
        files=store.files('bronze/'+dataset)
        if {f['path']:f['bytes'] for f in files}!={f['key']:f['bytes'] for f in inventory['files']}:raise RuntimeError('Bronze inventory changed')
        if not files:raise RuntimeError('Empty source dataset')
        coverage={}
        if dataset!='execution':
            for f in files:
                day=int(f['path'].split('/day=')[1].split('/')[0]);hour=int(f['path'].split('/hour=')[1].split('/')[0])
                coverage.setdefault(day,set()).add(hour)
            expected=set(range(109,116)) if dataset=='network' else set(range(185))
            if not marker.get('synthetic') and (set(coverage)!=expected or any(v!=set(range(24)) for v in coverage.values())):
                raise RuntimeError('Whole-trace day/hour coverage mismatch')
        summary={'coverage':{str(k):sorted(v) for k,v in coverage.items()},'files':len(files),'rows':0,'columns':{name:{'logical_type':dtype,'physical_types':{},'known_null_rows':0,'row_groups_missing_statistics':0,'min':None,'max':None,'unsafe_integer_range_groups':0,'integer_range_requires_row_check_groups':0} for name,dtype in SCHEMAS[dataset].items()},'schema_variants':{}}
        variants=Counter()
        with ThreadPoolExecutor(max_workers=16) as pool:
            jobs=[pool.submit(inspect,store,dataset,item) for item in files]
            for done,future in enumerate(as_completed(jobs),1):
                result=future.result();summary['rows']+=result['rows']
                signature=json.dumps(result['schema'],sort_keys=True);variants[signature]+=1
                c=result['columns']
                if dataset=='pod' and all(c[n]['min']==0 and c[n]['max']==0 for n in ['gpu_request','cpu_request_cores','used_gpu_hours']) and (c['avg_gpu_sm_util']['max'] or 0)>0:
                    report['observations'].append({'dataset':dataset,'source_key':result['key'],'rows':result['rows'],
                      'code':'zero_requests_with_positive_gpu_activity','interpretation':'Review source snapshot consistency; do not treat this hour as proven zero demand or idle capacity.',
                      'evidence':{n:c[n] for n in ['gpu_request','cpu_request_cores','used_gpu_hours','avg_gpu_sm_util','avg_cpu_request_util','avg_memory_util']}})

                if not result['compatible']:report['errors'].append({'dataset':dataset,'key':result['key'],'schema':result['schema']})
                for name,column in result['columns'].items():
                    if name not in summary['columns']:continue
                    target=summary['columns'][name];types=target['physical_types'];kind=column['physical_type'];types[kind]=types.get(kind,0)+1
                    for key in ['known_null_rows','row_groups_missing_statistics','unsafe_integer_range_groups','integer_range_requires_row_check_groups']:target[key]+=column[key]
                    for key,fn in [('min',min),('max',max)]:
                        value=column[key]
                        if value is not None:
                            try:target[key]=value if target[key] is None else fn(target[key],value)
                            except TypeError:target[key]=None
                    if column['unsafe_integer_range_groups']:report['errors'].append({'dataset':dataset,'key':result['key'],'column':name,'error':'integer exceeds exact double range'})
                if done%500==0:print(dataset,done,'/',len(files),flush=True)
        summary['schema_variants']=[{'schema':json.loads(key),'files':count} for key,count in variants.items()]
        report['tables'][dataset]=summary
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2,allow_nan=False,default=str))
        print(dataset,'complete',len(files),'files',summary['rows'],'rows',flush=True)
    report.update(status='passed' if not report['errors'] else 'failed',elapsed_seconds=time.monotonic()-started)
    args.output.write_text(json.dumps(report,indent=2,allow_nan=False,default=str))
    if report['errors']:raise RuntimeError('Whole-trace schema preflight found incompatible cases')

if __name__=='__main__':main()
