"""Tests for the transform-session execution batch size and the ``transform_batch_size`` knob."""

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
    _resolve_execution_batch_size,
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


def _udf_calls_for_transform(
    input_rows: int, *, batch_size: int | None, transform_batch_size: int | None = None
) -> int:
    """Run the real transform path on one input batch; return how many times the UDF ran."""
    registry = _CountingUDFRegistry()
    session = _create_transform_session(_TABLE_ID, registry, batch_size, transform_batch_size)
    batch = pa.record_batch(
        {
            "id": pa.array(list(range(input_rows)), type=pa.int64()),
            "name": pa.array([f"n{i}" for i in range(input_rows)], type=pa.string()),
        }
    )
    _bind_batch_table(session, _TABLE_ID, batch)
    session.sql(f"SELECT count_udf(id) AS id, name FROM {to_sql_identifier(_TABLE_ID)}").collect()
    return registry.calls


@pytest.mark.parametrize(
    "batch_size, transform_batch_size, expected",
    [
        (None, None, None),
        (128, None, None),  # small read batch must NOT shrink the transform batch
        (_DEFAULT, None, None),  # equal to default -> leave DataFusion alone
        (_DEFAULT + 1000, None, _DEFAULT + 1000),  # large read batch -> raise to honor it
        (128, 512, 512),  # explicit transform_batch_size wins, even below the default
        (_DEFAULT + 1000, 256, 256),  # explicit wins over the read-derived value
        (None, 128, 128),
    ],
)
def test_resolve_execution_batch_size(batch_size, transform_batch_size, expected):
    assert _resolve_execution_batch_size(batch_size, transform_batch_size) == expected


def test_small_read_batch_does_not_fragment_large_transform_input():
    """Regression guard: a small read batch_size must NOT slice a large input into tiny UDF calls."""
    # Before the fix this was ~64 calls (8192 / 128). After: a single call.
    assert _udf_calls_for_transform(_DEFAULT, batch_size=128) == 1


def test_explicit_transform_batch_size_is_honored():
    """The tunable knob takes effect: an explicit small transform_batch_size fragments as asked."""
    assert _udf_calls_for_transform(_DEFAULT, batch_size=None, transform_batch_size=128) == _DEFAULT // 128


def test_transform_batch_size_threads_through_split(tmp_path):
    """transform_batch_size set on a split reaches the transform session and fragments as asked."""
    num_rows = 2000
    table = pa.table(
        {
            "id": pa.array(list(range(num_rows)), type=pa.int64()),
            "name": pa.array([f"n{i}" for i in range(num_rows)], type=pa.string()),
        }
    )
    sql = f"SELECT count_udf(id) AS id, name FROM {to_sql_identifier(_TABLE_ID)}"

    # Default: large read batch (one scan batch), no transform override -> one UDF call.
    default_reg = _CountingUDFRegistry()
    default_split = _create_test_split(
        tmp_path,
        table,
        FileFormat.PARQUET,
        _TRANSFORM_SCHEMA,
        transform_sql=sql,
        table_id=_TABLE_ID,
        udf_registry=default_reg,
        filename="default.parquet",
    )
    list(default_split)
    assert default_reg.calls == 1

    # Explicit transform_batch_size=128 -> input sliced into ceil(2000/128) UDF calls.
    tuned_reg = _CountingUDFRegistry()
    tuned_split = _create_test_split(
        tmp_path,
        table,
        FileFormat.PARQUET,
        _TRANSFORM_SCHEMA,
        transform_sql=sql,
        table_id=_TABLE_ID,
        udf_registry=tuned_reg,
        transform_batch_size=128,
        filename="tuned.parquet",
    )
    list(tuned_split)
    assert tuned_reg.calls == math.ceil(num_rows / 128)
