from collections.abc import Iterator
from dataclasses import dataclass

from datafusion.plan import LogicalPlan
from pyiceberg.io import FileScanTask

from openhouse.dataloader.data_loader_split import DataLoaderSplit
from openhouse.dataloader.udf_registry import UDFRegistry


@dataclass
class DataLoaderSplits:
    """Iterable collection of data splits for parallel data loading

    Args:
        _logical_plan: Logical plan for query to be executed on each split before returning the data
        _udf_registry: UDF registry for UDFs in the query to be executed
        _splits: Iterable of data splits to load the table
    """

    _logical_plan: LogicalPlan
    _udf_registry: UDFRegistry
    _splits: list[FileScanTask]

    def __iter__(self) -> Iterator[DataLoaderSplit]:
        for file_scan_task in self._splits:
            yield DataLoaderSplit(self._logical_plan, file_scan_task, self._udf_registry)
