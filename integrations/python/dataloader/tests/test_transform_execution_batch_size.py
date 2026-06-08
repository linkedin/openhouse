"""Tests for clamping the transform session's ``datafusion.execution.batch_size``."""

from __future__ import annotations

import math

import pyarrow as pa
import pytest
from datafusion import udf
from datafusion.context import SessionContext
from pyiceberg.manifest import FileFormat

from openhouse.dataloader.data_loader_split import (
    _DATAFUSION_DEFAULT_BATCH_SIZE,
    _bind_batch_table,
    _create_transform_session,
    to_sql_identifier,
)
from openhouse.dataloader.table_identifier import TableIdentifier
from openhouse.dataloader.udf_registry import UDFRegistry
from tests.test_data_loader_split import _TRANSFORM_SCHEMA, _create_test_split

_TABLE_ID = TableIdentifier("db", "tbl")
_DEFAULT = _DATAFUSION_DEFAULT_BATCH_SIZE  # 8192


class _CountingUDFRegistry(UDFRegistry):
    """Registers an identity UDF on ``id`` that records each invocation (one per batch)."""

    def __init__(self) -> None:
        self.calls = 0

    def register_udfs(self, session_context: SessionContext) -> None:
        def _identity(arr: pa.Array) -> pa.Array:
            self.calls += 1
            return arr

        session_context.register_udf(udf(_identity, [pa.int64()], pa.int64(), "immutable", name="count_udf"))


def _udf_calls_for_transform(input_rows: int, *, batch_size: int | None) -> int:
    """Run the real transform path on one input batch; return how many times the UDF ran."""
    registry = _CountingUDFRegistry()
    session = _create_transform_session(_TABLE_ID, registry, batch_size)
    batch = pa.record_batch(
        {
            "id": pa.array(list(range(input_rows)), type=pa.int64()),
            "name": pa.array([f"n{i}" for i in range(input_rows)], type=pa.string()),
        }
    )
    _bind_batch_table(session, _TABLE_ID, batch)
    session.sql(f"SELECT count_udf(id) AS id, name FROM {to_sql_identifier(_TABLE_ID)}").collect()
    return registry.calls


@pytest.mark.parametrize("batch_size", [None, 128, _DEFAULT])
def test_small_read_batch_does_not_fragment_transform(batch_size):
    """A read batch_size at or below the default must NOT slice a large input into tiny UDF calls.

    Before the clamp a batch_size of 128 set datafusion.execution.batch_size=128, fragmenting
    a 8192-row input into 64 UDF calls. With the clamp, DataFusion stays at its default and
    the input is processed in a single call.
    """
    assert _udf_calls_for_transform(_DEFAULT, batch_size=batch_size) == 1


def test_large_read_batch_is_honored():
    """A read batch_size above the default raises datafusion.execution.batch_size to match,
    so a single large input batch is not fragmented at the 8192 default."""
    large = _DEFAULT + 1000
    # Without the raise this input would be sliced into ceil(large / _DEFAULT) == 2 calls.
    assert _udf_calls_for_transform(large, batch_size=large) == 1


def test_large_read_batch_threads_through_split(tmp_path):
    """A read batch_size above the default reaches the transform session via the split,
    keeping a single large input batch in one UDF call."""
    num_rows = _DEFAULT + 1000
    table = pa.table(
        {
            "id": pa.array(list(range(num_rows)), type=pa.int64()),
            "name": pa.array([f"n{i}" for i in range(num_rows)], type=pa.string()),
        }
    )
    sql = f"SELECT count_udf(id) AS id, name FROM {to_sql_identifier(_TABLE_ID)}"

    registry = _CountingUDFRegistry()
    split = _create_test_split(
        tmp_path,
        table,
        FileFormat.PARQUET,
        _TRANSFORM_SCHEMA,
        transform_sql=sql,
        table_id=_TABLE_ID,
        udf_registry=registry,
        batch_size=num_rows,
        filename="threaded.parquet",
    )
    list(split)
    assert registry.calls == math.ceil(num_rows / num_rows) == 1
