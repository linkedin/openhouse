from collections.abc import Iterator, Mapping

from datafusion.plan import LogicalPlan
from pyarrow import RecordBatch
from pyiceberg.io import FileScanTask

from openhouse.dataloader.udf_registry import UDFRegistry


class DataLoaderSplit:
    """A single data split"""

    def __init__(
        self,
        plan: LogicalPlan,
        file_scan_task: FileScanTask,
        udf_registry: UDFRegistry,
        table_properties: Mapping[str, str],
    ):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry
        self._table_properties = table_properties

    @property
    def table_properties(self) -> Mapping[str, str]:
        """Properties of the table being loaded"""
        return self._table_properties

    def __iter__(self) -> Iterator[RecordBatch]:
        """Loads the split data after applying, including applying a prerequisite
        table transformation if provided

        Returns:
            An iterator for batches of data in the split
        """
        raise NotImplementedError
