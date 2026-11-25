"""Verify the checked-in AWS evidence snapshot without AWS credentials."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'docs/evidence'


def require(condition, message):
    if not condition:
        raise SystemExit('FAIL: ' + message)


def main():
    checksums = json.loads((EVIDENCE / 'checksums.json').read_text())
    for relative, expected in checksums['sha256'].items():
        path = ROOT / relative
        require(path.is_file(), 'missing ' + relative)
        require(hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                'changed evidence: ' + relative)
    exports = EVIDENCE / 'aws/2026-11-20'
    bronze = [json.loads(p.read_text()) for p in exports.glob('bronze-*-success.json')]
    require({x['dataset'] for x in bronze} == {'pod', 'server', 'network', 'execution'},
            'four Bronze datasets required')
    require(sum(x['file_count'] for x in bronze) == 9049, 'Bronze file total')
    code_hash = hashlib.sha256((ROOT / 'pipelines/silver.py').read_bytes()).hexdigest()
    sample_names = ['silver-pod-day109.json', 'silver-server-day109.json',
                    'silver-network-day109.json', 'silver-execution-global.json']
    silver = [json.loads((exports / name).read_text()) for name in sample_names]
    require(len(silver) == 4, 'four sampled production manifests required')
    for manifest in silver:
        require(manifest['synthetic_source'] is False, 'synthetic manifest')
        q = manifest['quality']
        require(q['input_rows'] == q['accepted_rows'] + q['quarantined_rows'],
                'sample row accounting: ' + manifest['dataset'])
        require(bool(manifest['spark_application_id']), 'Spark application identity')
        require(manifest['code_sha256'] == code_hash,
                'Silver source changed since evidence capture; review and document provenance')
    status = json.loads((exports / 'silver-run-status.json').read_text())
    require(status['run_id'] == 'silver_full_20261119_r2', 'unexpected run')
    require(status['expected_transform_partitions'] == 378, 'partition plan')
    deployed = json.loads((exports / 'gold-deployment-verification.json').read_text())
    require(deployed['Status'] == 'Success', 'Gold deployment verification')
    gold_hash = hashlib.sha256((ROOT / 'pipelines/gold.py').read_bytes()).hexdigest()
    require(gold_hash in deployed['StandardOutputContent'], 'Gold code differs from deployment checkpoint')
    final = EVIDENCE / 'aws/2026-11-21'
    result = json.loads((final / 'gold-run-status.json').read_text())
    export = json.loads((final / 'gold-export-manifest.json').read_text())
    stop = json.loads((final / 'gold-service-result.json').read_text())
    worker = json.loads((final / 'ec2-stopped-snapshot.json').read_text())
    require(result.get('verified_complete') and result['dag_state'] == 'success', 'Gold completion')
    require(result['gold_day_partitions'] == 185 and result['task_states'] == {'success': 186}, 'Gold scope')
    require(export['code_sha256'] == gold_hash, 'Gold export code identity')
    require(result['bundle_bytes'] == export['total_bytes'] == sum(x['bytes'] for x in export['files'].values()), 'export size accounting')
    require(len(export['tables']) == 7, 'seven exported Gold tables')
    require(stop['service_result'] == 'success' and stop['exit_status'] == '0', 'successful finalizer')
    require(worker['State'] == 'stopped', 'stopped EC2 checkpoint')
    print('PASS: Gold cloud completion, export metadata and stopped EC2 checkpoint.')
    local_path = EVIDENCE / 'local-gold-verification.json'
    if local_path.exists():
        local = json.loads(local_path.read_text())
        require(local['status'] == 'passed' and local['daily_inventory_checks'] == 185 and local['mix_dimension_checks'] == 5, 'local Gold verification record')
        print('PASS: local Gold integrity/consolidation verification is recorded.')
        print('Use scripts/verify_gold_bundle.py to recheck the local data itself.')
    else:
        print('Local data download/hash/metric verification remains deferred.')
    print('PASS: Gold deployed-code identity.')
    print('PASS: evidence hashes, 9,049 Bronze files, four real-source Silver samples,')
    print('      row reconciliation and Silver code identity.')
    print('Checkpoint:', status['updated_utc'], '| DAG:', status['dag_state'])
    print('Final completion verified:', bool(status.get('verified_complete', False)))
    print('Snapshot consistency is verified; AWS origin is not independently attested.')


if __name__ == '__main__':
    main()
