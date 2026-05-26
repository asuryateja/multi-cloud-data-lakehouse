"""
AWS Delta Lake Utilities — Multi-Cloud Lakehouse
Implements ACID upsert/merge, schema enforcement, time travel, and vacuum
patterns using the delta-rs Python library on S3.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, TableNotFoundError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
S3_BUCKET    = os.environ.get("DELTA_LAKE_BUCKET", "mc-lakehouse-delta-lake-dev")
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")

STORAGE_OPTS = {
    "AWS_REGION":                   AWS_REGION,
    "AWS_S3_ALLOW_UNSAFE_RENAME":   "true",     # Required for Delta on S3
}


def _table_uri(layer: str, table_name: str) -> str:
    return f"s3://{S3_BUCKET}/{layer}/{table_name}"


# ─── Schema Registry ──────────────────────────────────────────────────────────

EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",             pa.string(),              nullable=False),
    pa.field("event_timestamp",      pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("user_id",              pa.string(),              nullable=True),
    pa.field("session_id",           pa.string(),              nullable=True),
    pa.field("event_type",           pa.string(),              nullable=False),
    pa.field("source_system",        pa.string(),              nullable=False),
    pa.field("country_code",         pa.string(),              nullable=True),
    pa.field("device_type",          pa.string(),              nullable=True),
    pa.field("payload",              pa.large_string(),        nullable=True),
    pa.field("_ingestion_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_partition_date",      pa.date32(),              nullable=False),
])


# ─── Write Operations ──────────────────────────────────────────────────────────

def write_bronze(
    data: pa.Table,
    partition_date: str,
    mode: str = "append",
) -> None:
    """
    Write raw data to the Delta Lake bronze layer with schema enforcement.
    Partitioned by _partition_date for efficient pruning.
    """
    uri = _table_uri("bronze", "events")

    # Enforce schema — reject any writes with mismatched columns
    if data.schema != EVENTS_SCHEMA:
        extra_cols  = set(data.schema.names) - set(EVENTS_SCHEMA.names)
        missing_cols = set(EVENTS_SCHEMA.names) - set(data.schema.names)
        raise ValueError(
            f"Schema mismatch writing to bronze/events. "
            f"Extra: {extra_cols}, Missing: {missing_cols}"
        )

    log.info("Writing %d rows to bronze/events partition=%s", len(data), partition_date)
    write_deltalake(
        table_or_uri=uri,
        data=data,
        partition_by=["_partition_date"],
        mode=mode,
        schema_mode="merge",       # Allow additive schema evolution
        storage_options=STORAGE_OPTS,
        engine="rust",             # delta-rs Rust engine for best S3 perf
        max_rows_per_group=1_000_000,
        configuration={
            "delta.dataSkippingNumIndexedCols": "5",
            "delta.checkpoint.writeStatsAsJson": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
        },
    )
    log.info("Write complete.")


def upsert_silver(
    updates: pa.Table,
    merge_keys: list[str],
    table_name: str,
    layer: str = "silver",
) -> dict[str, Any]:
    """
    Merge (upsert) records into a Delta table using ACID merge semantics.
    Matched rows are updated; unmatched rows are inserted.
    Returns merge operation metrics.
    """
    uri = _table_uri(layer, table_name)

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)
    except TableNotFoundError:
        log.info("Table %s does not exist; creating with initial write.", uri)
        write_deltalake(uri, updates, mode="overwrite", storage_options=STORAGE_OPTS)
        return {"created": True, "rows_inserted": len(updates)}

    # Build merge predicate from keys
    predicate = " AND ".join(
        f"source.{k} = target.{k}" for k in merge_keys
    )

    log.info("Merging %d rows into %s on keys=%s", len(updates), uri, merge_keys)

    result = (
        dt.merge(
            source=updates,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    log.info(
        "Merge complete: %d inserted, %d updated, %d deleted",
        result.get("num_target_rows_inserted", 0),
        result.get("num_target_rows_updated", 0),
        result.get("num_target_rows_deleted", 0),
    )
    return result


# ─── Time Travel ──────────────────────────────────────────────────────────────

def read_as_of(
    table_name: str,
    layer: str,
    as_of: datetime | None = None,
    version: int | None = None,
    filters: list[tuple] | None = None,
) -> pa.Table:
    """
    Read a Delta table at a specific point in time or version.
    Supports predicate pushdown via pyarrow filter expressions.
    """
    uri = _table_uri(layer, table_name)

    if as_of is not None:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)
        dt.load_as_version(as_of)
        log.info("Loaded %s as of %s (version %d)", uri, as_of, dt.version())
    elif version is not None:
        dt = DeltaTable(uri, version=version, storage_options=STORAGE_OPTS)
        log.info("Loaded %s at version %d", uri, version)
    else:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)

    return dt.to_pyarrow_table(filters=filters)


def get_table_history(table_name: str, layer: str, limit: int = 10) -> list[dict]:
    """Return the last `limit` Delta transaction log entries."""
    uri = _table_uri(layer, table_name)
    dt  = DeltaTable(uri, storage_options=STORAGE_OPTS)
    return dt.history(limit=limit)


# ─── Maintenance ──────────────────────────────────────────────────────────────

def optimize_and_vacuum(
    table_name: str,
    layer: str,
    target_size_mb: int = 256,
    retention_hours: int = 168,  # 7 days
) -> None:
    """
    Run Delta OPTIMIZE (bin-packing + Z-ORDER) then VACUUM to reclaim storage.
    Z-ORDER on high-cardinality columns used frequently in query predicates.
    """
    uri = _table_uri(layer, table_name)
    dt  = DeltaTable(uri, storage_options=STORAGE_OPTS)

    log.info("Running OPTIMIZE on %s (target file size %dMB)", uri, target_size_mb)
    metrics = dt.optimize.compact(
        target_size=target_size_mb * 1024 * 1024,
        max_concurrent_tasks=8,
    )
    log.info("Optimize complete: %s", metrics)

    # Z-ORDER on columns commonly used in WHERE clauses
    log.info("Running Z-ORDER on %s", uri)
    dt.optimize.z_order(columns=["user_id", "event_type", "_partition_date"])

    log.info("Running VACUUM (retention %dh) on %s", retention_hours, uri)
    vacuum_result = dt.vacuum(
        retention_hours=retention_hours,
        dry_run=False,
        enforce_retention_duration=True,
    )
    log.info("Vacuum removed %d files.", len(vacuum_result))


# ─── Schema Evolution ─────────────────────────────────────────────────────────

def add_column(
    table_name: str,
    layer: str,
    column_name: str,
    column_type: pa.DataType,
    nullable: bool = True,
) -> None:
    """Perform additive schema evolution by adding a new nullable column."""
    uri = _table_uri(layer, table_name)
    dt  = DeltaTable(uri, storage_options=STORAGE_OPTS)

    current_schema = dt.schema().to_pyarrow()
    if column_name in current_schema.names:
        log.warning("Column '%s' already exists in %s. Skipping.", column_name, uri)
        return

    new_schema = current_schema.append(pa.field(column_name, column_type, nullable=nullable))
    # Write empty batch to trigger schema update via schema_mode=merge
    empty = pa.table({name: pa.array([], type=field.type) for name, field in zip(new_schema.names, new_schema)})
    write_deltalake(uri, empty, mode="append", schema_mode="merge", storage_options=STORAGE_OPTS)
    log.info("Added column '%s' (%s) to %s.", column_name, column_type, uri)
