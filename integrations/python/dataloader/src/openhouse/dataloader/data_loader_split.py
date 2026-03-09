from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from types import MappingProxyType

from datafusion.context import SessionContext
from datafusion.plan import LogicalPlan
from datafusion.substrait import Producer
from pyarrow import RecordBatch
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import FileScanTask

from openhouse.dataloader._table_scan_context import TableScanContext
from openhouse.dataloader.udf_registry import NoOpRegistry, UDFRegistry


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        file_scan_task: FileScanTask,
        scan_context: TableScanContext,
        plan: LogicalPlan | None = None,
        session_context: SessionContext | None = None,
        udf_registry: UDFRegistry | None = None,
    ):
        self._plan = plan
        self._session_context = session_context
        self._plan_substrait_bytes: bytes | None = None
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry or NoOpRegistry()
        self._scan_context = scan_context

    @property
    def id(self) -> str:
        """Unique ID for the split. This is stable across executions for a given
        snapshot and split size.
        """
        file_path = self._file_scan_task.file.file_path
        return hashlib.sha256(file_path.encode("utf-8")).hexdigest()

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return MappingProxyType(self._scan_context.table_metadata.properties)

    def __iter__(self) -> Iterator[RecordBatch]:
        """Reads the file scan task and yields Arrow RecordBatches.

        Uses PyIceberg's ArrowScan to handle format dispatch, schema resolution,
        delete files, and partition spec lookups.
        """
        ctx = self._scan_context
        arrow_scan = ArrowScan(
            table_metadata=ctx.table_metadata,
            io=ctx.io,
            projected_schema=ctx.projected_schema,
            row_filter=ctx.row_filter,
        )
        yield from arrow_scan.to_record_batches([self._file_scan_task])

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        if state.get("_plan") is not None and state.get("_session_context") is not None:
            substrait_plan = Producer.to_substrait_plan(state["_plan"], state["_session_context"])
            state["_plan_substrait_bytes"] = substrait_plan.encode()
        state.pop("_plan", None)
        state.pop("_session_context", None)
        return state

    def __setstate__(self, state: dict) -> None:
        state.setdefault("_plan", None)
        state.setdefault("_session_context", None)
        state.setdefault("_plan_substrait_bytes", None)
        self.__dict__.update(state)
