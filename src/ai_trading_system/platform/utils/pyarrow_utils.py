from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from core.logging import logger


def read_parquet(
    path: str | Path,
    columns: Optional[List[str]] = None,
    filters: Optional[List[tuple] | List[List[tuple]]] = None,
) -> pa.Table:
    """Read parquet file(s) with optional column selection and filters."""
    path = str(path)
    if "*" in path or "?" in path:
        return ds.dataset(
            path,
            format="parquet",
            partitioning="hive",
        ).to_table(columns=columns, filter=pc.and_(*filters) if filters else None)
    return pq.read_table(path, columns=columns)


def write_parquet(
    table: pa.Table,
    path: str | Path,
    compression: str = "zstd",
    row_group_size: Optional[int] = None,
) -> str:
    """Write pyarrow table to parquet with compression."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq_writer = pa.ParquetWriter(
        path,
        table.schema,
        compression=compression,
        row_group_size=row_group_size,
    )
    pq_writer.write_table(table)
    pq_writer.close()
    return str(path)


def append_parquet(
    table: pa.Table,
    path: str | Path,
) -> str:
    """Append pyarrow table to existing parquet file."""
    path = Path(path)
    if path.exists():
        existing = pq.read_table(path)
        combined = pa.concat_tables([existing, table])
        return write_parquet(combined, path)
    return write_parquet(table, path)


def merge_parquet_files(
    input_pattern: str | Path,
    output_path: str | Path,
    compression: str = "zstd",
) -> str:
    """Merge multiple parquet files into one."""
    input_pattern = str(input_pattern)
    dataset = ds.dataset(input_pattern, format="parquet", partitioning="hive")
    table = dataset.to_table()
    return write_parquet(table, output_path, compression=compression)


def compact_parquet(
    directory: str | Path,
    output_name: str,
    partition_by: Optional[str] = None,
) -> str:
    """Compact all parquet files in directory into single file."""
    directory = Path(directory)
    files = list(directory.glob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files found in {directory}")
        return ""

    logger.info(f"Compacting {len(files)} files from {directory}")
    tables = [pa.ipc.open_file(f).read_all() for f in files]
    combined = pa.concat_tables(tables)
    output_path = directory / output_name
    return write_parquet(combined, output_path)


def get_parquet_stats(path: str | Path) -> dict:
    """Get row count and size stats for parquet file(s)."""
    path = Path(path)
    if path.is_file():
        metadata = pq.read_metadata(path)
        return {
            "rows": metadata.num_rows,
            "num_row_groups": metadata.num_row_groups,
            "size_bytes": path.stat().st_size,
            "path": str(path),
        }
    files = list(path.glob("*.parquet"))
    total_rows = 0
    total_size = 0
    for f in files:
        metadata = pq.read_metadata(f)
        total_rows += metadata.num_rows
        total_size += f.stat().st_size
    return {
        "num_files": len(files),
        "total_rows": total_rows,
        "total_size_bytes": total_size,
        "path": str(path),
    }
