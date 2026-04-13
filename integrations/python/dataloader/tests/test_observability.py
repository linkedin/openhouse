"""Tests for the performance observability module."""

from __future__ import annotations

import os
import pickle
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
    EnrichingObserver,
    LoggingPerfObserver,
    NullPerfObserver,
    PerfConfig,
    PerfEvent,
    PerfObserver,
    bootstrap_observer,
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


# --- PerfConfig tests ---


def test_perf_config_defaults():
    config = PerfConfig()
    assert dict(config.tags) == {}
    assert config.observer_type == "logging"
    assert dict(config.observer_kwargs) == {}


def test_perf_config_is_frozen():
    config = PerfConfig(tags={"k": "v"})
    with pytest.raises(AttributeError):
        config.observer_type = "null"  # type: ignore[misc]


def test_perf_config_pickle_round_trip():
    config = PerfConfig(tags={"cluster": "prod", "tenant": "t1"}, observer_type="null", observer_kwargs={"x": 1})
    restored = pickle.loads(pickle.dumps(config))
    assert restored.tags["cluster"] == "prod"
    assert restored.tags["tenant"] == "t1"
    assert restored.observer_type == "null"
    assert restored.observer_kwargs["x"] == 1


def test_perf_config_pickle_default():
    """Default PerfConfig survives pickle round-trip."""
    config = PerfConfig()
    restored = pickle.loads(pickle.dumps(config))
    assert dict(restored.tags) == {}
    assert restored.observer_type == "logging"


# --- EnrichingObserver tests ---


def test_enriching_observer_prepends_session_tags():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    inner = Collector()
    enriching = EnrichingObserver(inner, {"cluster": "prod", "tenant": "t1"})
    event = PerfEvent(operation="test.enrich", duration_ms=1.0, tags={"extra": "yes"})
    enriching.emit(event)

    assert len(events) == 1
    assert events[0].tags["cluster"] == "prod"
    assert events[0].tags["tenant"] == "t1"
    assert events[0].tags["extra"] == "yes"


def test_enriching_observer_per_event_tags_override_session():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    inner = Collector()
    enriching = EnrichingObserver(inner, {"env": "staging"})
    event = PerfEvent(operation="test.override", duration_ms=1.0, tags={"env": "prod"})
    enriching.emit(event)

    assert events[0].tags["env"] == "prod"


def test_enriching_observer_empty_session_tags():
    events: list[PerfEvent] = []

    class Collector:
        def emit(self, event: PerfEvent) -> None:
            events.append(event)

    inner = Collector()
    enriching = EnrichingObserver(inner, {})
    event = PerfEvent(operation="test.empty", duration_ms=1.0, tags={"k": "v"})
    enriching.emit(event)

    assert events[0].tags == {"k": "v"}


# --- bootstrap_observer tests ---


def test_bootstrap_observer_sets_logging_by_default():
    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        bootstrap_observer(PerfConfig())
        obs = get_observer()
        assert isinstance(obs, LoggingPerfObserver)
    finally:
        set_observer(original)


def test_bootstrap_observer_sets_enriching_with_tags():
    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        bootstrap_observer(PerfConfig(tags={"cluster": "prod"}))
        obs = get_observer()
        assert isinstance(obs, EnrichingObserver)
    finally:
        set_observer(original)


def test_bootstrap_observer_null_type():
    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        bootstrap_observer(PerfConfig(observer_type="null"))
        obs = get_observer()
        assert isinstance(obs, NullPerfObserver)
    finally:
        set_observer(original)


def test_bootstrap_observer_idempotent():
    """bootstrap_observer does not replace an already-configured observer."""
    original = get_observer()
    try:
        custom = _MockObserver()
        set_observer(custom)
        bootstrap_observer(PerfConfig(observer_type="null"))
        assert get_observer() is custom
    finally:
        set_observer(original)


def test_bootstrap_observer_dotted_import_path():
    """bootstrap_observer can instantiate a custom observer from a dotted import path."""
    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        # Use LoggingPerfObserver via its dotted path as a stand-in for a custom observer
        bootstrap_observer(PerfConfig(observer_type="openhouse.dataloader._observability.LoggingPerfObserver"))
        obs = get_observer()
        assert isinstance(obs, LoggingPerfObserver)
    finally:
        set_observer(original)


def test_bootstrap_observer_dotted_import_with_tags():
    """Dotted import observer gets wrapped with EnrichingObserver when tags are provided."""
    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        bootstrap_observer(
            PerfConfig(
                tags={"env": "test"},
                observer_type="openhouse.dataloader._observability.LoggingPerfObserver",
            )
        )
        obs = get_observer()
        assert isinstance(obs, EnrichingObserver)
    finally:
        set_observer(original)


# --- Integration: PerfConfig with data loader ---


def test_data_loader_with_perf_config_tags(tmp_path):
    """Session tags from PerfConfig appear on split-level perf events."""
    catalog = _make_real_catalog(tmp_path)
    observer = _MockObserver()

    original = get_observer()
    try:
        set_observer(observer)
        config = PerfConfig(tags={"cluster": "prod", "tenant": "t1"})

        loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", perf_config=config)
        batches = [batch for split in loader for batch in split]
        assert len(batches) >= 1

        # split_iter events should carry the session tags
        split_event = next(e for e in observer.events if e.operation == "dataloader.split_iter")
        assert split_event.tags["cluster"] == "prod"
        assert split_event.tags["tenant"] == "t1"
    finally:
        set_observer(original)


def test_data_loader_perf_config_transform_tags(tmp_path):
    """Session tags appear on transform perf events when PerfConfig is used."""
    catalog = _make_real_catalog(tmp_path)
    observer = _MockObserver()

    original = get_observer()
    try:
        set_observer(observer)
        config = PerfConfig(tags={"env": "staging"})

        loader = OpenHouseDataLoader(
            catalog=catalog,
            database="db",
            table="tbl",
            context=DataLoaderContext(table_transformer=_MaskingTransformer()),
            perf_config=config,
        )
        batches = [batch for split in loader for batch in split]
        assert len(batches) >= 1

        # Transform events should also carry the session tags
        transform_event = next(e for e in observer.events if e.operation == "dataloader.apply_transform")
        assert transform_event.tags["env"] == "staging"

        session_event = next(e for e in observer.events if e.operation == "dataloader.create_transform_session")
        assert session_event.tags["env"] == "staging"
    finally:
        set_observer(original)


def test_pickle_split_with_perf_config(tmp_path):
    """A pickled DataLoaderSplit carries its PerfConfig and bootstraps on the worker side."""
    catalog = _make_real_catalog(tmp_path)

    original = get_observer()
    try:
        set_observer(NullPerfObserver())
        config = PerfConfig(tags={"cluster": "test"})

        loader = OpenHouseDataLoader(catalog=catalog, database="db", table="tbl", perf_config=config)
        splits = list(loader)
        assert len(splits) == 1

        # Pickle round-trip the split (simulating send to worker)
        pickled = pickle.dumps(splits[0])
        restored_split = pickle.loads(pickled)

        # Simulate fresh worker: reset to NullPerfObserver
        set_observer(NullPerfObserver())
        assert isinstance(get_observer(), NullPerfObserver)

        # Iterating the restored split should bootstrap the observer
        set_observer(NullPerfObserver())

        # The split's __iter__ calls bootstrap_observer which replaces the NullPerfObserver
        batches = list(restored_split)
        assert len(batches) >= 1

        # After iteration, observer should no longer be NullPerfObserver
        obs = get_observer()
        assert not isinstance(obs, NullPerfObserver)
    finally:
        set_observer(original)
