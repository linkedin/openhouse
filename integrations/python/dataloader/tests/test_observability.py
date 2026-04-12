"""Tests for the performance observability module."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField, StringType

from openhouse.dataloader._observability import (
    CompositeObserver,
    LoggingPerfObserver,
    NullPerfObserver,
    PerfEvent,
    PerfObserver,
    get_observer,
    perf_timer,
    set_observer,
)
from openhouse.dataloader.data_loader import DataLoaderContext, OpenHouseDataLoader
from openhouse.dataloader.data_loader_split import to_sql_identifier
from openhouse.dataloader.table_transformer import TableTransformer

# --- PerfEvent tests ---


def test_perf_event_to_dict():
    event = PerfEvent(operation="test.op", duration_ms=42.5, tags={"key": "val"}, metrics={"rows": 10})
    d = event.to_dict()
    assert d == {"operation": "test.op", "duration_ms": 42.5, "key": "val", "rows": 10}


def test_perf_event_to_dict_empty():
    event = PerfEvent(operation="test.op", duration_ms=1.0)
    d = event.to_dict()
    assert d == {"operation": "test.op", "duration_ms": 1.0}


def test_perf_event_tags_and_metrics_are_separate():
    event = PerfEvent(operation="test.op", duration_ms=1.0, tags={"db": "prod"}, metrics={"rows": 42})
    assert "db" not in event.metrics
    assert "rows" not in event.tags


# --- Observer tests ---


def test_null_observer_does_not_raise():
    obs = NullPerfObserver()
    obs.emit(PerfEvent(operation="noop", duration_ms=0.0))


def test_logging_observer_logs_at_debug(caplog):
    import logging

    with caplog.at_level(logging.DEBUG, logger="openhouse.dataloader.perf"):
        obs = LoggingPerfObserver()
        obs.emit(PerfEvent(operation="test.log", duration_ms=5.0, metrics={"x": 1}))

    assert len(caplog.records) == 1
    assert "test.log" in caplog.records[0].message


def test_composite_observer_fans_out():
    events_a: list[PerfEvent] = []
    events_b: list[PerfEvent] = []

    class CollectorA:
        def emit(self, event: PerfEvent) -> None:
            events_a.append(event)

    class CollectorB:
        def emit(self, event: PerfEvent) -> None:
            events_b.append(event)

    composite = CompositeObserver(CollectorA(), CollectorB())
    event = PerfEvent(operation="fan.out", duration_ms=1.0)
    composite.emit(event)

    assert len(events_a) == 1
    assert len(events_b) == 1
    assert events_a[0] is event
    assert events_b[0] is event


def test_null_observer_satisfies_protocol():
    assert isinstance(NullPerfObserver(), PerfObserver)


def test_logging_observer_satisfies_protocol():
    assert isinstance(LoggingPerfObserver(), PerfObserver)


# --- set_observer / get_observer tests ---


def test_set_and_get_observer():
    original = get_observer()
    try:
        custom = NullPerfObserver()
        set_observer(custom)
        assert get_observer() is custom
    finally:
        set_observer(original)


def test_init_does_not_override_custom_observer(tmp_path):
    """Creating an OpenHouseDataLoader does not replace a user-configured observer."""
    catalog = _make_real_catalog(tmp_path)
    observer = _MockObserver()

    original = get_observer()
    try:
        set_observer(observer)
        OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")
        assert get_observer() is observer
    finally:
        set_observer(original)


# --- perf_timer tests ---


def test_perf_timer_emits_event():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    original = get_observer()
    try:
        set_observer(Collector())
        with perf_timer("test.timer", extra="val"):
            pass

        assert len(events) == 1
        assert events[0].operation == "test.timer"
        assert events[0].duration_ms >= 0
        assert events[0].tags["extra"] == "val"
    finally:
        set_observer(original)


def test_perf_timer_tag_and_metric():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    original = get_observer()
    try:
        set_observer(Collector())
        with perf_timer("test.set") as ctx:
            ctx.tag("db", "prod")
            ctx.metric("count", 42)

        assert events[0].tags["db"] == "prod"
        assert events[0].metrics["count"] == 42
    finally:
        set_observer(original)


def test_perf_timer_emits_on_exception():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    original = get_observer()
    try:
        set_observer(Collector())
        with pytest.raises(ValueError, match="boom"), perf_timer("test.error"):
            raise ValueError("boom")

        assert len(events) == 1
        assert events[0].operation == "test.error"
        assert events[0].duration_ms >= 0
    finally:
        set_observer(original)


# --- Integration: data loader emits perf events ---


COL_ID = "id"
COL_NAME = "name"

TEST_SCHEMA = Schema(
    NestedField(field_id=1, name=COL_ID, field_type=LongType(), required=False),
    NestedField(field_id=2, name=COL_NAME, field_type=StringType(), required=False),
)

TEST_DATA = {
    COL_ID: [1, 2, 3],
    COL_NAME: ["alice", "bob", "charlie"],
}


def _write_parquet(tmp_path, data: dict, filename: str = "test.parquet") -> str:
    file_path = str(tmp_path / filename)
    table = pa.table(data)
    fields = [field.with_metadata({b"PARQUET:field_id": str(i + 1).encode()}) for i, field in enumerate(table.schema)]
    pq.write_table(table.cast(pa.schema(fields)), file_path)
    return file_path


def _make_real_catalog(tmp_path, data: dict = TEST_DATA, iceberg_schema: Schema = TEST_SCHEMA):
    file_path = _write_parquet(tmp_path, data)

    metadata = new_table_metadata(
        schema=iceberg_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        location=str(tmp_path),
        properties={},
    )
    io = load_file_io(properties={}, location=file_path)

    data_file = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=len(next(iter(data.values()))),
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file._spec_id = 0
    task = FileScanTask(data_file=data_file)

    def fake_scan(**kwargs):
        scan = MagicMock()
        scan.projection.return_value = iceberg_schema
        scan.plan_files.return_value = [task]
        return scan

    mock_table = MagicMock()
    mock_table.metadata = metadata
    mock_table.io = io
    mock_table.scan.side_effect = fake_scan

    catalog = MagicMock()
    catalog.load_table.return_value = mock_table
    return catalog


class _MockObserver:
    """Test observer that collects all emitted events."""

    def __init__(self) -> None:
        self.events: list[PerfEvent] = []

    def emit(self, event: PerfEvent) -> None:
        self.events.append(event)


def test_data_loader_iter_emits_perf_events(tmp_path):
    """Full iteration through data loader emits expected perf events."""
    catalog = _make_real_catalog(tmp_path)
    observer = _MockObserver()

    original = get_observer()
    try:
        set_observer(observer)

        loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl")

        batches = [batch for split in loader for batch in split]
        assert len(batches) >= 1

        operations = [e.operation for e in observer.events]
        assert "dataloader.resolve_snapshot" in operations
        assert "dataloader.iter" in operations
        assert "dataloader.split_iter" in operations

        # Check iter event has split_count
        iter_event = next(e for e in observer.events if e.operation == "dataloader.iter")
        assert iter_event.metrics["split_count"] == 1
        assert iter_event.duration_ms > 0

        # Check split_iter has batch/row counts
        split_event = next(e for e in observer.events if e.operation == "dataloader.split_iter")
        assert split_event.metrics["batch_count"] >= 1
        assert split_event.metrics["row_count"] == 3
        assert split_event.duration_ms > 0
    finally:
        set_observer(original)


class _MaskingTransformer(TableTransformer):
    def __init__(self):
        super().__init__(dialect="datafusion")

    def transform(self, table, context):
        return f"SELECT id, 'MASKED' as name FROM {to_sql_identifier(table)}"


def test_data_loader_transform_emits_perf_events(tmp_path):
    """Transform path emits build_query, create_transform_session, and apply_transform events."""
    catalog = _make_real_catalog(tmp_path)
    observer = _MockObserver()

    original = get_observer()
    try:
        set_observer(observer)

        loader = OpenHouseDataLoader(
            catalog=catalog,
            database="db",
            table="tbl",
            context=DataLoaderContext(table_transformer=_MaskingTransformer()),
        )

        batches = [batch for split in loader for batch in split]
        assert len(batches) >= 1

        operations = [e.operation for e in observer.events]
        assert "dataloader.build_query" in operations
        assert "dataloader.create_transform_session" in operations
        assert "dataloader.apply_transform" in operations

        # Check apply_transform has row counts
        transform_event = next(e for e in observer.events if e.operation == "dataloader.apply_transform")
        assert transform_event.metrics["input_rows"] == 3
        assert transform_event.metrics["output_rows"] == 3
        assert transform_event.duration_ms > 0
    finally:
        set_observer(original)
