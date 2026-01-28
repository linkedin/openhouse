from datafusion.context import SessionContext
from datafusion.plan import LogicalPlan
from typing import Iterator
from pyarrow import RecordBatch
from pyiceberg.io import FileScanTask
from openhouse.dataloader.udf_registry import UDFRegistry


class Worker:
    def __init__(self, udf_registry: UDFRegistry):
        self._udf_registry = udf_registry

    def load_data(self, plan: LogicalPlan, input: FileScanTask) -> Iterator[RecordBatch]:
        """Loads data from the file with the query plan applied

        Args:
            plan: The query plan to execute on the data
            input: The file scan task to read from

        Returns:
            An iterator of RecordBatch for the data in the input with the plan applied
        """
        ctx = SessionContext()
        self._udf_registry.register_udfs(ctx)
        pass