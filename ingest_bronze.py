"""Stream original Parquet members from local ZIPs into verified S3 Bronze."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
from zipfile import ZipFile

TABLES = {'network': 'asi_opensource_network_hourly', 'server': 'asi_opensource_server_hourly',
          'pod': 'asi_opensource_pod_hourly', 'execution': 'asi_opensource_job_execution_summary'}


class ForwardReader:
    """Expose sequential reads so multipart upload does not scan a ZIP member twice."""
    def __init__(self, reader):
        self.reader = reader

    def read(self, size=-1):
        return self.reader.read(size)


def inventory(archive, dataset):
    table = TABLES[dataset]
    pattern = (rf'{table}/([^/]+\.parquet)' if dataset == 'execution' else
               rf'{table}/day=(\d+)/hour=(\d{{1,2}})/([^/]+\.parquet)')
    entries = []
    keys = set()
    with ZipFile(archive) as source:
        for item in source.infolist():
            if item.is_dir():
                continue
            match = re.fullmatch(pattern, item.filename)
            if not match:
                raise ValueError(f'Unexpected ZIP member: {item.filename}')
            if dataset == 'execution':
                key = f'bronze/{dataset}/all/{match[1]}'
            else:
                day, hour = int(match[1]), int(match[2])
                if not 0 <= hour <= 23:
                    raise ValueError(f'Invalid hour: {hour}')
                key = f'bronze/{dataset}/day={day}/hour={hour:02}/{match[3]}'
            if key in keys:
                raise ValueError(f'Duplicate destination: {key}')
            keys.add(key)
            entries.append({'member': item.filename, 'key': key, 'bytes': item.file_size,
                            'crc32': f'{item.CRC:08x}'})
    if not entries:
        raise ValueError('Archive contains no Parquet files')
    if dataset != 'execution':
        expected_days = set(range(109, 116) if dataset == 'network' else range(185))
        coverage = {}
        for entry in entries:
            day, hour = re.search(r'/day=(\d+)/hour=(\d+)/', entry['key']).groups()
            coverage.setdefault(int(day), set()).add(int(hour))
        if set(coverage) != expected_days or any(hours != set(range(24)) for hours in coverage.values()):
            raise ValueError(f'Incomplete day/hour coverage in {dataset}')
    return entries


def ingest(dataset, archive_dir, bucket, workers=4):
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import ClientError
    archive = Path(archive_dir) / f'{TABLES[dataset]}.zip'
    entries = inventory(archive, dataset)
    client = boto3.client('s3')
    prefix = f'bronze/{dataset}/'
    manifest_key = f'manifests/bronze/{dataset}.json'
    success_key = f'manifests/bronze/{dataset}.success.json'
    client.delete_object(Bucket=bucket, Key=success_key)
    config = TransferConfig(use_threads=False, multipart_chunksize=16 * 1024**2)

    def upload(entry):
        metadata = {'source-crc32': entry['crc32'], 'source-member': entry['member']}
        try:
            head = client.head_object(Bucket=bucket, Key=entry['key'])
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
                raise
            head = {}
        if head.get('ContentLength') == entry['bytes'] and head.get('Metadata') == metadata:
            return 'skipped'
        with ZipFile(archive) as source, source.open(entry['member']) as reader:
            # Reading to EOF validates the ZIP member's CRC before upload succeeds.
            client.upload_fileobj(ForwardReader(reader), bucket, entry['key'], ExtraArgs={'Metadata': metadata}, Config=config)
        head = client.head_object(Bucket=bucket, Key=entry['key'])
        if head['ContentLength'] != entry['bytes'] or head['Metadata'] != metadata:
            raise ValueError(f'Object verification failed: {entry["key"]}')
        return 'uploaded'

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for index, result in enumerate(pool.map(upload, entries), 1):
            results.append(result)
            if index % 100 == 0 or index == len(entries):
                print(json.dumps({'dataset': dataset, 'completed_files': index, 'total_files': len(entries)}), flush=True)
    expected = {entry['key']: entry['bytes'] for entry in entries}
    actual = {}
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        actual.update({item['Key']: item['Size'] for item in page.get('Contents', [])})
    # Remove only obsolete files inside the selected dataset's Bronze prefix.
    obsolete = set(actual) - set(expected)
    for key in obsolete:
        client.delete_object(Bucket=bucket, Key=key)
        del actual[key]
    if actual != expected:
        raise ValueError('S3 inventory differs from source archive')
    manifest = {'dataset': dataset, 'archive': archive.name, 'files': entries,
                'file_count': len(entries), 'total_bytes': sum(expected.values()),
                'uploaded': results.count('uploaded'), 'skipped': results.count('skipped'),
                'verification': 'source CRC on upload, destination size and metadata, full key/size inventory'}
    client.put_object(Bucket=bucket, Key=manifest_key, Body=json.dumps(manifest).encode(), ContentType='application/json')
    client.put_object(Bucket=bucket, Key=success_key, Body=json.dumps({k: v for k, v in manifest.items() if k != 'files'}).encode(), ContentType='application/json')
    print(json.dumps({k: v for k, v in manifest.items() if k != 'files'}), flush=True)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dataset', choices=TABLES)
    parser.add_argument('--archive-dir', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 9))
    args = parser.parse_args()
    ingest(args.dataset, args.archive_dir, args.bucket, args.workers)
