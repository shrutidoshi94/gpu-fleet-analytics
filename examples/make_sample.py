"""Create a small synthetic network archive with the upstream directory layout."""

import io
from pathlib import Path
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

folder = Path("sample")
folder.mkdir(exist_ok=True)
table = "asi_opensource_network_hourly"
with ZipFile(folder / f"{table}.zip", "w") as archive:
    for hour in range(24):
        buffer = io.BytesIO()
        pq.write_table(
            pa.table(
                {
                    "server_id": ["demo-a", "demo-b"],
                    "rx_gibps_avg": [hour / 48, hour / 40],
                    "tx_gibps_avg": [hour / 72, hour / 60],
                }
            ),
            buffer,
        )
        archive.writestr(f"{table}/day=109/hour={hour:02}/part-000.parquet", buffer.getvalue())
