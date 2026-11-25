"""Bounded real-data benchmark, NEVER a full backfill. Run on chosen EC2 worker.
Writes nine immutable Silver attempts (four pod pilots, three server pilots,
network, execution), plus one inventory audit per daily supporting table. Retains logs.
"""
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[1] / 'pipelines'))
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from silver import Store, CODE_SHA256, source_inventory


def guarded(command, env, args, started, log):
    import signal
    peak=0
    minimum=shutil.disk_usage(args.results).free
    process=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    try:
        while process.poll() is None:
            snapshot=subprocess.check_output(['ps','-eo','pgid=,rss='],text=True)
            rss=sum(int(line.split()[1]) for line in snapshot.splitlines() if line.split() and int(line.split()[0])==process.pid)
            peak=max(peak,rss)
            free=shutil.disk_usage(args.results).free
            minimum=min(minimum,free)
            if free/2**30<args.min_free_gib or time.monotonic()-started>args.max_seconds:
                raise RuntimeError(f'Benchmark stopped at disk/time guard: {free/2**30:.2f} GiB free')
            time.sleep(2)
        if process.returncode:
            raise RuntimeError(f'Benchmark process failed ({process.returncode}); inspect log')
        return peak,minimum
    finally:
        if process.poll() is None:
            os.killpg(process.pid,signal.SIGTERM)
            try: process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL); process.wait()


def reusable(store, report, driver_memory):
    if report.get('code_sha256') != CODE_SHA256 or report.get('driver_memory') != driver_memory:
        return False
    day=0 if report['dataset']=='execution' else report['day']
    if source_inventory(store,report['dataset'],day)[2] != report['source_fingerprint']:
        return False
    return all(store.files(report[area]) == report['output_files'][area] for area in ('accepted','quarantine'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True)
    parser.add_argument('--results',type=Path,required=True)
    parser.add_argument('--threads',type=int,default=6)
    parser.add_argument('--driver-memory',default='12g')
    parser.add_argument('--max-seconds',type=int,default=5400,help='Whole benchmark deadline, includes startup')
    parser.add_argument('--min-free-gib',type=float,default=10)
    parser.add_argument('--resume',action='store_true',help='Validate and reuse completed cases from results.json')
    args=parser.parse_args()
    args.results.mkdir(parents=True,exist_ok=True)
    events=(args.results/'spark-events').resolve(); events.mkdir(exist_ok=True)
    store=Store(args.root)
    cases=[('server',3,args.threads),('server',109,args.threads),('server',173,args.threads),('pod',3,args.threads),('pod',109,args.threads),
           ('pod',173,args.threads),('pod',109,max(1,args.threads//2)),
           ('network',109,args.threads),('execution',0,args.threads)]
    start=time.monotonic()
    reports=[]
    audit_reports=[]
    saved=json.loads((args.results/'results.json').read_text()) if args.resume and (args.results/'results.json').exists() else {}
    resumed=0
    env=os.environ.copy()
    env['PYSPARK_SUBMIT_ARGS']=f'--driver-memory {args.driver_memory} pyspark-shell'
    env.setdefault('SPARK_LOCAL_DIRS',str((args.results/'spark-scratch').resolve()))
    for dataset,day,threads in cases:
        name=f'{dataset}-{day}-{threads}threads'
        prior=next((r for r in saved.get('reports',[]) if r['benchmark_case']==name),None)
        if prior and reusable(store,prior,args.driver_memory):
            reports.append(prior); resumed+=1
            print(json.dumps({'case':name,'reused_validated_measurement':True}),flush=True)
            continue
        command=[sys.executable,str(Path(__file__).resolve().parents[1]/'pipelines/silver.py'),dataset,str(day),
                 '--root',args.root,'--master',f'local[{threads}]','--force',
                 '--event-log-dir',events.as_uri()]
        began=time.monotonic()
        log_path=args.results/f'{name}.log'
        if log_path.exists():
            log_path.rename(log_path.with_name(log_path.name+f'.prior-{int(time.time())}'))
        with log_path.open('w') as log:
            peak_rss_kib,min_free_bytes=guarded(command,env,args,start,log)
        part='all' if dataset=='execution' else f'day={day}'
        report=store.read(f'manifests/silver/{dataset}/{part}.current.json')
        report.update(benchmark_case=name,wall_seconds=time.monotonic()-began,threads=threads,
                      driver_memory=args.driver_memory,remaining_disk_bytes=shutil.disk_usage(args.results).free)
        report.update(peak_process_group_rss_kib=peak_rss_kib,min_free_disk_bytes=min_free_bytes,
                      output_bytes=sum(f['bytes'] for area in report['output_files'].values() for f in area))
        metrics={'executor_cpu_ns':0,'executor_run_ms':0,'gc_ms':0,'memory_spilled_bytes':0,'disk_spilled_bytes':0,'peak_execution_memory_bytes':0}
        for path in events.glob(report['spark_application_id']+'*'):
            if not path.is_file(): continue
            for line in path.open():
                event=json.loads(line)
                if event.get('Event')!='SparkListenerTaskEnd': continue
                task=event.get('Task Metrics',{})
                for field,label in [('Executor CPU Time','executor_cpu_ns'),('Executor Run Time','executor_run_ms'),('JVM GC Time','gc_ms'),('Memory Bytes Spilled','memory_spilled_bytes'),('Disk Bytes Spilled','disk_spilled_bytes')]:
                    metrics[label]+=task.get(field,0)
                metrics['peak_execution_memory_bytes']=max(metrics['peak_execution_memory_bytes'],task.get('Peak Execution Memory',0))
        report['spark_task_metrics']=metrics
        reports.append(report)
        (args.results/'results.json').write_text(json.dumps({'status':'running','reports':reports},indent=2))
        print(json.dumps({'case':name,'wall_seconds':report['wall_seconds'],'source_bytes':report['source_bytes']}),flush=True)
    # The previous pod109 audit may refer to a prior inventory; resolve again.
    for dataset,day in [('pod',3),('pod',109),('pod',173),('network',109)]:
        command=[sys.executable,str(Path(__file__).resolve().parents[1]/'pipelines/silver.py'),dataset,str(day),
                 '--root',args.root,'--audit-only','--master',f'local[{args.threads}]']
        began=time.monotonic()
        with (args.results/f'audit-{dataset}-{day}.log').open('w') as log:
            peak,minimum=guarded(command,env,args,start,log)
        audit_reports.append({'dataset':dataset,'day':day,'wall_seconds':time.monotonic()-began,
                              'peak_process_group_rss_kib':peak,'min_free_disk_bytes':minimum})
    (args.results/'results.json').write_text(json.dumps({'status':'complete','reports':reports,'session_wall_seconds':time.monotonic()-start,'resumed_cases':resumed,'audit_reports':audit_reports},indent=2))
    print('Benchmark complete; no other trace days were scheduled.',flush=True)

if __name__=='__main__': main()
