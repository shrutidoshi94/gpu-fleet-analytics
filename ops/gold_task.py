"""Bounded Gold subprocesses, reuse validated outputs and pilot budget gate."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from silver import Store
from gold import CODE_SHA
from ops.silver_task import guarded


def main():
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['pin','day','context','publish','export','pilot-check'])
    p.add_argument('--release',required=True);p.add_argument('--root',required=True)
    p.add_argument('--day',type=int);p.add_argument('--days')
    args=p.parse_args();store=Store(args.root)
    folder=Path(os.environ.get('TRACE_GOLD_LOG_DIR',f'logs/gold/{args.release}'));folder.mkdir(parents=True,exist_ok=True)
    tag=f'{args.action}-{args.day if args.day is not None else "all"}'
    plan=store.read(f'manifests/gold/{args.release}/plan.json')
    if args.action=='pilot-check':
        reports=[json.loads(x.read_text()) for x in folder.glob('day-*.json')]
        selected=[x for x in reports if x.get('release')==args.release and x.get('status')=='success']
        if not selected:raise ValueError('Pilot reports required')
        mean=sum(x['elapsed_seconds'] for x in selected)/len(selected)
        estimate=mean*len(plan['days'])/3600
        report={'release':args.release,'pilot_days':[x['day'] for x in selected],
                'projected_daily_hours':estimate,'budget_hours':12,'status':'passed' if estimate<=12 else 'blocked'}
        store.write(f'manifests/gold/{args.release}/pilot.json',report)
        (folder/'pilot.json').write_text(json.dumps(report,indent=2))
        if estimate>12:raise ValueError('Pilot projects beyond12-hour daily processing budget; review before full run')
        return
    # Reuse only an exact-plan, current-code attempt with verified output inventories.
    if args.action in ('day','context') and plan:
        unit=f'day={args.day}' if args.action=='day' else 'global'
        m=store.read(f'manifests/gold/{args.release}/{unit}.current.json')
        if m and m['plan_identity']==plan['identity'] and m['code_sha256']==CODE_SHA and all(store.files(t['path'])==t['files'] for t in m['tables'].values()):
            print('Reused verified Gold '+unit);return
    if shutil.disk_usage(folder).free<10*2**30:raise RuntimeError('Gold10-GiB disk guard')
    if args.action=='export':
        cmd=[sys.executable,'ops/export_gold.py','--root',args.root,'--release',args.release,'--destination',str(folder/'offline-bundle')]
    else:
        cmd=[sys.executable,'gold.py',args.action,'--root',args.root,'--release',args.release,'--master',os.environ.get('TRACE_SPARK_MASTER','local[6]')]
        if args.day is not None:cmd+=['--day',str(args.day)]
        if args.days:cmd+=['--days',args.days]
    started=time.monotonic()
    result={'release':args.release,'action':args.action,'day':args.day,'status':'failed'}
    try:
        with (folder/(tag+'.log')).open('w') as stream:
            guarded(cmd,stream,folder,max_seconds=3600,poll_seconds=3)
        result['status']='success'
    finally:
        result.update(elapsed_seconds=time.monotonic()-started,free_disk_gib_after=shutil.disk_usage(folder).free/2**30)
        (folder/(tag+'.json')).write_text(json.dumps(result,indent=2))
        store.write(f'run-logs/gold/{args.release}/{tag}.json',result)

if __name__=='__main__':main()
