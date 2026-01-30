from collections.abc import Iterator

from datafusion.plan import LogicalPlan
from pyarrow import RecordBatch
from pyiceberg.io import FileScanTask

from openhouse.dataloader.udf_registry import UDFRegistry


class DataLoaderSplit:
    """A single data split"""

    def __init__(self, plan: LogicalPlan, file_scan_task: FileScanTask, udf_registry: UDFRegistry):
        self._plan = plan
        self._file_scan_task = file_scan_task
        self._udf_registry = udf_registry

    def __call__(self) -> Iterator[RecordBatch]:
        """Loads the split data after applying, including applying a prerequisite
        table transformation if provided

        Returns:
            An iterator for batches of data in the split
        """
        raise NotImplementedError
