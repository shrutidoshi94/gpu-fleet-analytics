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
    code_hash = hashlib.sha256((ROOT / 'silver.py').read_bytes()).hexdigest()
    silver = [json.loads(p.read_text()) for p in exports.glob('silver-*.json')
              if p.name != 'silver-run-status.json']
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
    print('PASS: evidence hashes, 9,049 Bronze files, four real-source Silver samples,')
    print('      row reconciliation and Silver code identity.')
    print('Checkpoint:', status['updated_utc'], '| DAG:', status['dag_state'])
    print('Final completion verified:', bool(status.get('verified_complete', False)))
    print('Snapshot consistency is verified; AWS origin is not independently attested.')


if __name__ == '__main__':
    main()
