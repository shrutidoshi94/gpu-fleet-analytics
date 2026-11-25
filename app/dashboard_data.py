"""Pure, local Gold readers and capacity-planning calculations. No AWS clients."""
from pathlib import Path
import hashlib
import json
import math
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

TABLES = ('capacity_hourly','activity_evidence_hourly','workload_mix_hourly','placement_daily',
          'planning_quality','network_context_hourly','execution_context_global')


def verify_bundle(folder):
    folder=Path(folder); manifest=json.loads((folder/'bundle.json').read_text())
    expected={'capacity_hourly','activity_evidence_hourly','workload_mix_hourly','placement_daily',
              'planning_quality','network_context_hourly','execution_context_global'}
    if set(manifest['tables'])!=expected:raise ValueError('Unexpected Gold products')
    for name,info in manifest['files'].items():
        if Path(name).name!=name:raise ValueError('Unsafe bundle path')
        path=folder/name
        if path.stat().st_size!=info['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest()!=info['sha256']:
            raise ValueError('Bundle integrity failed: '+name)
    for name,info in manifest['tables'].items():
        parquet=pq.ParquetFile(folder/(name+'.parquet'))
        if parquet.metadata.num_rows!=info['rows'] or parquet.schema_arrow.names!=info['columns']:
            raise ValueError('Table contract failed: '+name)
    return manifest


def independent_checks(folder):
    """Check consolidation against immutable daily reports using Arrow, not Spark."""
    folder=Path(folder);m=verify_bundle(folder);lineage=json.loads((folder/'lineage.json').read_text())
    c=pq.read_table(folder/'capacity_hourly.parquet',columns=['day','inventory_gpu_slots'])
    checked=0
    for day in m['relative_days']:
        actual=pc.sum(c.filter(pc.equal(c['day'],day))['inventory_gpu_slots']).as_py() or 0
        expected=lineage['release']['units'][f'day={day}']['quality']['inventory_gpu_slot_hours']
        if actual!=expected:raise ValueError(f'Inventory consolidation differs on day {day}')
        checked+=1
    quality=pq.read_table(folder/'planning_quality.parquet',columns=['observations'])
    observed=pc.sum(quality['observations']).as_py()
    expected=sum(v['quality']['source_accepted_rows'] for k,v in lineage['release']['units'].items() if k.startswith('day='))
    if observed!=expected:raise ValueError('Workload observations did not reconcile')
    mix=pq.read_table(folder/'workload_mix_hourly.parquet',columns=['dimension','observations'])
    for dimension in ('priority_class','job_type_public','model_type_public','state_public','is_genai_request'):
        if pc.sum(mix.filter(pc.equal(mix['dimension'],dimension))['observations']).as_py()!=observed:
            raise ValueError('Mix dimension does not reconcile: '+dimension)
    return {'status':'passed','release':m['release'],'verified_files':len(m['files']),
            'verified_tables':len(m['tables']),'daily_inventory_checks':checked,
            'reconciled_pod_observations':observed,'mix_dimension_checks':5,
            'table_rows':{k:v['rows'] for k,v in m['tables'].items()},
            'expected_file_bytes':m['total_bytes'],'aws_required_for_dashboard':False,
            'method':'SHA256, Arrow footer/schema checks, Arrow sums vs pinned daily reports and five mix populations'}


def read_table(folder,name,columns=None,filters=None):
    if name not in TABLES:raise ValueError('Unknown Gold product')
    table=pq.read_table(Path(folder)/(name+'.parquet'),columns=columns,filters=filters)
    return table.to_pandas(strings_to_categorical=True)


def select(frame,days,model=None,cluster=None,exclude_issues=False):
    mask=frame['day'].between(*days) if 'day' in frame else pd.Series(True,index=frame.index)
    if model is not None:mask &= frame['gpu_spec_public'].eq(model)
    if cluster is not None and 'cluster_id' in frame:mask &= frame['cluster_id'].eq(cluster)
    if exclude_issues:
        mask &= ~frame['source_anomaly'].fillna(True) & ~frame['duplicate_candidates_present'].fillna(True)
    return frame.loc[mask].copy()


def safe_divide(num,den):
    return num/den if den is not None and pd.notna(den) and den>0 else float('nan')


def hourly(frame):
    """Sum observed requests by hour; simultaneity is unverified. Unknown stays null."""
    if frame.empty:return pd.DataFrame()
    columns=['inventory_gpu_slots','compatible_observed_gpu_requests','compatible_valid_request_rows',
             'inventory_server_hours','valid_capacity_server_hours','servers_with_pod_observations',
             'matched_pod_observations','valid_request_rows','mismatched_server_hours']
    h=frame.groupby(['day','hour'],observed=True)[columns].sum(min_count=1).reset_index()
    h.loc[h.compatible_valid_request_rows.eq(0),'compatible_observed_gpu_requests']=np.nan
    h['request_ratio']=h.compatible_observed_gpu_requests/h.inventory_gpu_slots.replace(0,np.nan)
    h['trace_day']=h.day+h.hour/24
    return h.sort_values(['day','hour'])


def cohort_summary(frame,pressure=0.85):
    rows=[]
    for model,group in frame.groupby('gpu_spec_public',observed=True,dropna=False):
        h=hourly(group);ratio=h.request_ratio.dropna()
        rows.append({'GPU cohort':str(model),'Latest inventory slots':h.inventory_gpu_slots.iloc[-1],
            'p95 observed request slots':h.compatible_observed_gpu_requests.quantile(.95),
            'p95 request / inventory':ratio.quantile(.95),
            'Hours above review threshold':float((ratio>=pressure).mean()) if len(ratio) else np.nan,
            'Hours with request evidence':int(h.compatible_observed_gpu_requests.notna().sum()),
            'Server-hour observation coverage':safe_divide(h.servers_with_pod_observations.sum(),h.inventory_server_hours.sum())})
    return pd.DataFrame(rows).sort_values('p95 request / inventory',ascending=False) if rows else pd.DataFrame()


def eligible_activity(activity,capacity,days,model=None,cluster=None,exclude=True):
    a=select(activity,days,model,cluster)
    a=a.loc[a.inventory_match & a.cohort_match].copy()
    if exclude and not a.empty:
        allowed=select(capacity,days,model,cluster,True)[['day','hour','gpu_spec_public','cluster_id']]
        a=a.merge(allowed,on=['day','hour','gpu_spec_public','cluster_id'],how='inner',validate='one_to_one')
    return a


def activity_review(activity,threshold=20,minimum_coverage=.5):
    records=[]
    for model,g in activity.groupby('gpu_spec_public',observed=True):
        h=g.groupby(['day','hour'],observed=True)[['sm_source_unit_sum','sm_valid_observations','gpu_requesting_observations',
                                                  'observed_gpu_requests','reported_gpu_hours']].sum().reset_index()
        h['sm']=h.sm_source_unit_sum/h.sm_valid_observations.replace(0,np.nan)
        h['coverage']=h.sm_valid_observations/h.gpu_requesting_observations.replace(0,np.nan)
        measured=h.loc[(h.coverage>=minimum_coverage)&h.sm.notna()]
        low=measured.loc[measured.sm<threshold]
        records.append({'GPU cohort':str(model),'Measured source-unit SM mean':safe_divide(g.sm_source_unit_sum.sum(),g.sm_valid_observations.sum()),
            'SM observation coverage':safe_divide(g.sm_valid_observations.sum(),g.gpu_requesting_observations.sum()),
            'Eligible hours':len(measured),'Hours below review threshold':len(low),
            'Pattern share':safe_divide(len(low),len(measured)),
            'Reported GPU-hours in review hours':low.reported_gpu_hours.sum()})
    return pd.DataFrame(records).sort_values('Pattern share',ascending=False) if records else pd.DataFrame()


def scenario(frame,growth,reserve,anchor='p95',baseline='Latest observed hour'):
    if growth<0 or not 0<=reserve<1:raise ValueError('Invalid growth or reserve assumption')
    if frame.gpu_spec_public.dropna().nunique()!=1:raise ValueError('Select one GPU cohort; models are not interchangeable')
    clean=frame.loc[~frame.source_anomaly & ~frame.duplicate_candidates_present].copy()
    h=hourly(clean)
    if h.empty:return None
    h=h.loc[h.compatible_observed_gpu_requests.notna() & h.inventory_gpu_slots.gt(0)]
    if h.empty:return None
    demand=float(h.compatible_observed_gpu_requests.quantile(.95) if anchor=='p95' else h.compatible_observed_gpu_requests.max())
    inventory=hourly(frame)
    inventory=inventory.loc[inventory.inventory_gpu_slots.gt(0) & inventory.valid_capacity_server_hours.eq(inventory.inventory_server_hours)]
    if inventory.empty:return None
    reference=inventory.iloc[-1] if baseline=='Latest observed hour' else inventory.loc[inventory.inventory_gpu_slots.idxmin()]
    capacity=float(reference.inventory_gpu_slots)
    required=math.ceil(demand*(1+growth)/(1-reserve))
    return {'anchor':demand,'capacity':capacity,'modeled_slots':required,'additional_slots':max(0,required-capacity),
            'hours':len(h),'decision_ready':False,'baseline_day':int(reference.day),'baseline_hour':int(reference.hour)}
