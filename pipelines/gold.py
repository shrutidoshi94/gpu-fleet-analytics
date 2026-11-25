"""Capacity-planning Gold from pinned Silver; one serial writer per release.

Observed requests are evidence, not complete demand or procurement advice.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from silver import Store, CODE_SHA256 as SILVER_SHA

VERSION = 'gold-v1'
CODE_SHA = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
KEYS = ['day', 'hour', 'server_id']
COHORT = ['day', 'hour', 'gpu_spec_public', 'cluster_id']


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def valid(name):
    x = F.col(name)
    return x.isNotNull() & ~F.isnan(x.cast('double')) & (F.abs(x.cast('double')) != float('inf')) & (x >= 0)


def total(name, alias):
    return F.sum(F.when(valid(name), F.col(name)).otherwise(F.lit(0))).alias(alias)


def count_valid(name, alias):
    return F.sum(F.when(valid(name), 1).otherwise(0)).alias(alias)


def pin(store, release, days, synthetic=False):
    if not release.replace('-', '').replace('_', '').isalnum():
        raise ValueError('Release must contain only letters, digits, dash or underscore')
    if store.remote:
        status = store.read('run-logs/full-silver-20261119/status.json')
        if not status or not status.get('verified_complete') or status.get('dag_state') != 'success':
            raise ValueError('Verified complete production Silver required before Gold')
        if synthetic:
            raise ValueError('Synthetic bypass forbidden on S3')
    elif not synthetic:
        raise ValueError('Local source requires explicit synthetic fixture mode')
    snapshots = {}
    for ds in ('server', 'pod', 'network', 'execution'):
        covered = [0] if ds == 'execution' else [d for d in days if ds != 'network' or 109 <= d <= 115]
        for day in covered:
            part = 'all' if ds == 'execution' else f'day={day}'
            name = f'manifests/silver/{ds}/{part}.current.json'
            m = store.read(name)
            if not m or m['code_sha256'] != SILVER_SHA or bool(m['synthetic_source']) != synthetic:
                raise ValueError('Missing, wrong-code or wrong-source Silver: ' + name)
            q = m['quality']
            if q['input_rows'] != q['accepted_rows'] + q['quarantined_rows']:
                raise ValueError('Silver accounting failure')
            snapshots[name] = m
    value = {'version': VERSION, 'release': release, 'days': days, 'synthetic': synthetic,
             'code_sha256': CODE_SHA, 'silver_code_sha256': SILVER_SHA,
             'source_manifests': snapshots, 'created_utc': datetime.now(timezone.utc).isoformat(),
             'metric_policy': 'observed request evidence; procurement eligibility unverified'}
    value['identity'] = digest({k: value[k] for k in ('version', 'release', 'days', 'synthetic', 'code_sha256', 'source_manifests')})
    key = f'manifests/gold/{release}/plan.json'
    old = store.read(key)
    if old:
        if old['identity'] != value['identity']:
            raise ValueError('Existing release is pinned to different source or code; use a new release')
        return old
    store.write(key, value)
    return value


def source(plan, ds, day):
    part = 'all' if ds == 'execution' else f'day={day}'
    key = f'manifests/silver/{ds}/{part}.current.json'
    return key, plan['source_manifests'][key]


def frame(spark, store, plan, ds, day):
    _, m = source(plan, ds, day)
    return spark.read.parquet(store.uri(m['accepted'], spark=True))


def assert_source_current(store, plan, day=None):
    for key, value in plan['source_manifests'].items():
        if day is not None and f'/day={day}.' not in key:
            continue
        if store.read(key) != value:
            raise ValueError('Pinned Silver changed: ' + key)
        for area in ('accepted', 'quarantine'):
            if store.files(value[area]) != value['output_files'][area]:
                raise ValueError('Pinned Silver output inventory changed: ' + key)


def save_tables(store, plan, unit, tables, quality):
    if plan['code_sha256'] != CODE_SHA:
        raise ValueError('Gold code changed after pinning')
    assert_source_current(store, plan, int(unit[4:]) if unit.startswith('day=') else None)
    pointer = f'manifests/gold/{plan["release"]}/{unit}.current.json'
    old = store.read(pointer)
    if old and old['plan_identity'] == plan['identity'] and all(store.files(t['path']) == t['files'] for t in old['tables'].values()):
        return old
    attempt = f'gold_versions/{plan["release"]}/{unit}/{uuid.uuid4().hex}'
    result = {'version': VERSION, 'plan_identity': plan['identity'], 'unit': unit, 'code_sha256': CODE_SHA,
              'tables': {}, 'quality': quality, 'created_utc': datetime.now(timezone.utc).isoformat()}
    for name, data in tables.items():
        path = attempt + '/' + name
        data.coalesce(1).write.mode('error').option('compression', 'zstd').parquet(store.uri(path, spark=True))
        files = store.files(path)
        rows = sum(store.footer(f['path']).metadata.num_rows for f in files)
        result['tables'][name] = {'path': path, 'files': files, 'rows': rows,
                                 'schema': data.schema.jsonValue()}
    assert_source_current(store, plan, int(unit[4:]) if unit.startswith('day=') else None)
    store.write(attempt + '/report.json', result)
    store.write(pointer, result)
    return result


def daily(spark, store, plan, day):
    if day not in plan['days']:
        raise ValueError('Day not in pinned plan')
    p = frame(spark, store, plan, 'pod', day)
    inv = frame(spark, store, plan, 'server', day)
    pod_manifest = source(plan, 'pod', day)[1]
    inv_manifest = source(plan, 'server', day)[1]
    duplicates = pod_manifest['quality']['candidate_key_audit']['duplicate_groups']
    anomalies = pod_manifest['quality'].get('source_observations', [])
    anomaly_hours = sorted({int(m.group(1)) for x in anomalies for m in [re.search(r'/hour=(\d+)/', x.get('source_key',''))] if m})
    # Inventory uniqueness is a hard publication gate, independent of Silver QA.
    if inv.groupBy(*KEYS).count().filter('count != 1').limit(1).count():
        raise ValueError('Gold inventory keys are not unique')
    p = p.select(*KEYS, 'gpu_spec_public', 'cluster_id', 'gpu_request', 'used_gpu_hours',
                 'avg_gpu_sm_util', 'avg_gpu_mem_gib', 'avg_cpu_request_util', 'avg_memory_util',
                 'priority_class', 'job_type_public', 'model_type_public', 'state_public', 'is_genai_request')
    p = p.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # Workload rows are reduced before the unique inventory join: never fan out capacity.
        pods = p.groupBy(*KEYS).agg(F.count('*').alias('pod_observations'),
            total('gpu_request', 'observed_gpu_requests'), count_valid('gpu_request', 'valid_request_rows'),
            total('used_gpu_hours', 'reported_gpu_hours'),
            F.countDistinct('gpu_spec_public').alias('pod_model_variants'),
            F.count('gpu_spec_public').alias('pod_known_model_rows'),
            F.count('cluster_id').alias('pod_known_cluster_rows'),
            F.first('gpu_spec_public', ignorenulls=True).alias('pod_model'),
            F.countDistinct('cluster_id').alias('pod_cluster_variants'),
            F.first('cluster_id', ignorenulls=True).alias('pod_cluster'))
        joined = inv.select(*KEYS, 'gpu_spec_public', 'cluster_id', 'gpu_count').join(pods, KEYS, 'left')
        joined = joined.withColumn('model_cluster_mismatch', F.col('pod_observations').isNotNull() & F.coalesce(
            (F.col('pod_known_model_rows') != F.col('pod_observations')) |
            (F.col('pod_known_cluster_rows') != F.col('pod_observations')) |
            (F.col('pod_model_variants') != 1) | (F.col('pod_cluster_variants') != 1) |
            ~F.col('pod_model').eqNullSafe(F.col('gpu_spec_public')) |
            ~F.col('pod_cluster').eqNullSafe(F.col('cluster_id')), F.lit(False)))
        joined = joined.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            eligible = (F.col('pod_observations').isNotNull() &
                        (F.col('valid_request_rows') == F.col('pod_observations')) &
                        ~F.col('model_cluster_mismatch') & valid('gpu_count') &
                        ~F.col('hour').isin(anomaly_hours) & F.lit(duplicates == 0))
            joined = joined.withColumn('observed_headroom', F.when(eligible,
                F.col('gpu_count') - F.col('observed_gpu_requests')))
            capacity = joined.groupBy(*COHORT).agg(total('gpu_count', 'inventory_gpu_slots'),
                count_valid('gpu_count', 'valid_capacity_server_hours'), F.count('*').alias('inventory_server_hours'),
                F.count('pod_observations').alias('servers_with_pod_observations'),
                F.sum(F.coalesce('observed_gpu_requests', F.lit(0))).alias('matched_observed_gpu_requests'),
                F.sum(F.when(~F.col('model_cluster_mismatch'), F.coalesce('observed_gpu_requests', F.lit(0))).otherwise(0)).alias('compatible_observed_gpu_requests'),
                F.sum(F.when(~F.col('model_cluster_mismatch'), F.coalesce('valid_request_rows', F.lit(0))).otherwise(0)).alias('compatible_valid_request_rows'),
                F.sum(F.coalesce('pod_observations', F.lit(0))).alias('matched_pod_observations'),
                F.sum(F.coalesce('valid_request_rows', F.lit(0))).alias('valid_request_rows'),
                F.sum(F.when(F.col('model_cluster_mismatch'), 1).otherwise(0)).alias('mismatched_server_hours'),
                F.sum(F.when(F.col('observed_headroom') < 0, 1).otherwise(0)).alias('over_requested_server_hours'),
                F.count('observed_headroom').alias('headroom_evidenced_server_hours'),
                F.min('observed_headroom').alias('min_observed_headroom'),
                F.max('observed_headroom').alias('max_observed_headroom'))
            capacity = capacity.withColumn('inventory_gpu_slots',
                F.when(F.col('valid_capacity_server_hours') > 0, F.col('inventory_gpu_slots')))
            capacity = capacity.withColumn('partial_request_to_inventory_ratio',
                F.when((F.col('inventory_gpu_slots') > 0) & (F.col('compatible_valid_request_rows') > 0), F.col('compatible_observed_gpu_requests') / F.col('inventory_gpu_slots')))
            capacity = capacity.withColumn('source_anomaly', F.col('hour').isin(anomaly_hours))
            capacity = capacity.withColumn('duplicate_candidates_present', F.lit(duplicates > 0))
            capacity = capacity.withColumn('decision_ready', F.lit(False)).withColumn('eligibility_reason',
                F.lit('request/lifecycle completeness unverified; observed evidence only'))
            # Telemetry is aligned to inventory by key; mismatches remain a visible dimension.
            tagged = p.alias('p').join(inv.select(*KEYS, F.col('gpu_spec_public').alias('inventory_model'),
                  F.col('cluster_id').alias('inventory_cluster'), F.lit(True).alias('_inventory_key_exists')).alias('i'), KEYS, 'left')
            tagged = tagged.withColumn('inventory_match', F.coalesce(F.col('_inventory_key_exists'), F.lit(False)))
            tagged = tagged.withColumn('cohort_match', F.col('inventory_match') &
                F.col('inventory_model').isNotNull() & F.col('inventory_cluster').isNotNull() &
                F.col('gpu_spec_public').eqNullSafe(F.col('inventory_model')) &
                F.col('cluster_id').eqNullSafe(F.col('inventory_cluster')))
            tagged = tagged.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                activity = tagged.filter(valid('gpu_request') & (F.col('gpu_request') > 0)).groupBy(*COHORT, 'inventory_match', 'cohort_match').agg(
                    F.count('*').alias('gpu_requesting_observations'), total('gpu_request', 'observed_gpu_requests'),
                    total('avg_gpu_sm_util', 'sm_source_unit_sum'), count_valid('avg_gpu_sm_util', 'sm_valid_observations'),
                    total('avg_gpu_mem_gib', 'gpu_memory_gib_sum'), count_valid('avg_gpu_mem_gib', 'gpu_memory_valid_observations'),
                    total('avg_cpu_request_util', 'cpu_request_util_sum'), count_valid('avg_cpu_request_util', 'cpu_util_valid_observations'),
                    total('avg_memory_util', 'memory_util_sum'), count_valid('avg_memory_util', 'memory_util_valid_observations'),
                    total('used_gpu_hours', 'reported_gpu_hours'))
                mixes = []
                for dimension in ('priority_class', 'job_type_public', 'model_type_public', 'state_public', 'is_genai_request'):
                    mixes.append(tagged.withColumn('dimension', F.lit(dimension)).withColumn('value', F.col(dimension).cast('string'))
                        .groupBy(*COHORT, 'inventory_match', 'cohort_match', 'dimension', 'value').agg(
                            F.count('*').alias('observations'), total('gpu_request', 'observed_gpu_requests'),
                            count_valid('gpu_request', 'valid_request_observations')))
                mix = mixes[0]
                for data in mixes[1:]:
                    mix = mix.unionByName(data)
                placement = joined.withColumn('headroom_bin', F.when(F.col('observed_headroom').isNull(), 'unknown')
                    .when(F.col('observed_headroom') < 0, 'negative').when(F.col('observed_headroom') == 0, 'zero')
                    .when(F.col('observed_headroom') <= 1, '(0,1]').when(F.col('observed_headroom') <= 4, '(1,4]').otherwise('>4'))
                placement = placement.groupBy('day', 'gpu_spec_public', 'cluster_id', 'headroom_bin').count()
                quality = tagged.groupBy(*COHORT, 'inventory_match', 'cohort_match').agg(F.count('*').alias('observations'),
                    count_valid('gpu_request', 'valid_request_observations'))
                tables = {'capacity_hourly': capacity, 'activity_evidence_hourly': activity,
                          'workload_mix_hourly': mix, 'placement_daily': placement, 'planning_quality': quality}
                if 109 <= day <= 115:
                    n = frame(spark, store, plan, 'network', day)
                    n = n.join(inv.select(*KEYS, 'gpu_spec_public', 'cluster_id'), KEYS, 'left')
                    tables['network_context_hourly'] = n.groupBy(*COHORT).agg(F.count('*').alias('observations'),
                        total('rx_gibps_avg', 'rx_gibps_sum'), count_valid('rx_gibps_avg', 'rx_valid_observations'),
                        total('tx_gibps_avg', 'tx_gibps_sum'), count_valid('tx_gibps_avg', 'tx_valid_observations'))
                expected_capacity = inv.agg(total('gpu_count', 'slots')).first()['slots'] or 0
                actual_capacity = capacity.agg(F.sum('inventory_gpu_slots').alias('slots')).first()['slots'] or 0
                if expected_capacity != actual_capacity:
                    raise ValueError('Capacity fanout or loss detected')
                q = {'inventory_capacity_reconciled': True, 'inventory_gpu_slot_hours': actual_capacity,
                     'pod_duplicate_candidate_groups': duplicates, 'source_observations': anomalies,
                     'source_anomaly_hours': anomaly_hours, 'decision_ready': False,
                     'source_input_rows': pod_manifest['quality']['input_rows'],
                     'source_accepted_rows': pod_manifest['quality']['accepted_rows'],
                     'source_quarantined_rows': pod_manifest['quality']['quarantined_rows']}
                return save_tables(store, plan, f'day={day}', tables, q)
            finally:
                tagged.unpersist()
        finally:
            # unpersist the cached base even after projection replaces its Python reference.
            joined.unpersist()
    finally:
        p.unpersist()


def context(spark, store, plan):
    e = frame(spark, store, plan, 'execution', 0)
    table = e.groupBy('gpu_spec_public', 'priority_class', 'job_type_public').agg(
        F.count('*').alias('observed_execution_records'),
        total('duration_hours', 'observed_span_hours_sum'), count_valid('duration_hours', 'valid_span_records'),
        total('schedule_delay_sec', 'schedule_delay_sec_sum'), count_valid('schedule_delay_sec', 'valid_schedule_delay_records'),
        total('ready_delay_sec', 'ready_delay_sec_sum'), count_valid('ready_delay_sec', 'valid_ready_delay_records'),
        F.sum(F.when(F.col('ready_status') == True, 1).otherwise(0)).alias('ready_true_records'),
        F.sum(F.when(F.col('ready_status') == False, 1).otherwise(0)).alias('ready_false_records'),
        F.sum(F.when(F.col('ready_status').isNull(), 1).otherwise(0)).alias('ready_unknown_records'))
    return save_tables(store, plan, 'global', {'execution_context_global': table}, {'scope': 'global; no daily mapping'})


def publish(store, plan):
    assert_source_current(store, plan)
    units = {}
    for unit in [f'day={d}' for d in plan['days']] + ['global']:
        m = store.read(f'manifests/gold/{plan["release"]}/{unit}.current.json')
        if not m or m['plan_identity'] != plan['identity']:
            raise ValueError('Incomplete/mixed Gold release: ' + unit)
        for table in m['tables'].values():
            if store.files(table['path']) != table['files']:
                raise ValueError('Gold output changed')
        units[unit] = m
    result = {'release': plan['release'], 'version': VERSION, 'plan_identity': plan['identity'],
              'code_sha256': CODE_SHA, 'units': units, 'source_days': plan['days'],
              'scope': 'observed evidence; decision readiness unverified',
              'published_utc': datetime.now(timezone.utc).isoformat()}
    store.write(f'manifests/gold/{plan["release"]}/release.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['pin', 'day', 'context', 'publish'])
    parser.add_argument('--root', required=True)
    parser.add_argument('--release', required=True)
    parser.add_argument('--days', default=','.join(map(str, range(185))))
    parser.add_argument('--day', type=int)
    parser.add_argument('--synthetic', action='store_true')
    parser.add_argument('--master', default='local[6]')
    args = parser.parse_args()
    store = Store(args.root)
    if args.action == 'pin':
        days = sorted(set(map(int, args.days.split(','))))
        if not days or min(days) < 0 or max(days) > 184:
            raise ValueError('Days must be0–184')
        result = pin(store, args.release, days, args.synthetic)
    else:
        plan = store.read(f'manifests/gold/{args.release}/plan.json')
        if not plan or plan['code_sha256'] != CODE_SHA:
            raise ValueError('Pinned current-code plan required')
        if args.action == 'publish':
            result = publish(store, plan)
        else:
            builder = SparkSession.builder.master(args.master).appName(f'gold-{args.action}-{args.day}').config('spark.sql.shuffle.partitions', '16')
            if store.remote:
                builder = builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
            spark = builder.getOrCreate()
            try:
                result = daily(spark, store, plan, args.day) if args.action == 'day' else context(spark, store, plan)
            finally:
                spark.stop()
    print(json.dumps(result, allow_nan=False))


if __name__ == '__main__':
    main()
