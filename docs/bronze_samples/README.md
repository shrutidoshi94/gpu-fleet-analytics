# Actual Bronze row samples

Read-only S3 extraction on November 19, 2026; EC2 stayed stopped. Each file contains ten rows. Primary daily samples use day109/hour00; execution is global. `pod_gpu` is an additional first-ten-positive-GPU-request view from the same hour. None is a top-ranked or random sample. CSV blanks represent nulls; JSON retains exact null/type information and provenance. See the [Bronze profile](../data/BRONZE_PROFILE.md) and [column contracts](../data/COLUMN_CONTRACTS.md) for explanations.

- `pod`: `bronze/pod/day=109/hour=00/part-000.parquet`; source file 1,489,135 rows, 82,525,604 bytes.
- `server`: `bronze/server/day=109/hour=00/part-000.parquet`; source file 32,296 rows, 726,125 bytes.
- `network`: `bronze/network/day=109/hour=00/part-000.parquet`; source file 31,917 rows, 1,209,425 bytes.
- `execution`: `bronze/execution/all/part-000.parquet`; source file 40,522,321 rows, 1,188,294,827 bytes.
- `pod_gpu`: `bronze/pod/day=109/hour=00/part-000.parquet`; source file 1,489,135 rows, 82,525,604 bytes.
