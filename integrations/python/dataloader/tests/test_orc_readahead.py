import io
import pickle
import random
from unittest.mock import patch

import pyarrow as pa
import pyarrow.orc as orc
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from openhouse.dataloader import DataLoaderContext, OpenHouseDataLoader
from openhouse.dataloader._orc_readahead import OrcReadAheadFileIO, _ReadAheadFile
from openhouse.dataloader._table_scan_context import TableScanContext, _unpickle_scan_context
from tests.test_data_loader import _make_real_catalog
from tests.test_data_loader_split import _create_test_split

KB = 1024


class _CountingRaw(io.RawIOBase):
    """In-memory file that records every positional read as (offset, size)."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.reads: list[tuple[int, int]] = []

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._pos = offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        end = len(self._data) if size < 0 else min(len(self._data), self._pos + size)
        self.reads.append((self._pos, end - self._pos))
        out = self._data[self._pos : end]
        self._pos = end
        return out


def _payload(n: int) -> bytes:
    return bytes(i % 251 for i in range(n))


def _readahead(data: bytes, window: int, budget_windows: int = 16) -> tuple[_ReadAheadFile, _CountingRaw]:
    raw = _CountingRaw(data)
    return _ReadAheadFile(raw, len(data), window, budget_windows), raw


# --- _ReadAheadFile --------------------------------------------------------------------


def test_small_sequential_reads_are_coalesced_into_one_request():
    data = _payload(64 * KB)
    f, raw = _readahead(data, window=32 * KB)

    out = b"".join(f.read(1 * KB) for _ in range(32))

    assert out == data[: 32 * KB]
    assert raw.reads == [(0, 32 * KB)]


def test_reads_at_or_above_window_pass_through_unchanged():
    data = _payload(64 * KB)
    f, raw = _readahead(data, window=8 * KB)

    f.seek(4 * KB)
    assert f.read(16 * KB) == data[4 * KB : 20 * KB]
    assert raw.reads == [(4 * KB, 16 * KB)]


def test_read_spanning_window_boundary_returns_correct_bytes():
    data = _payload(64 * KB)
    f, _ = _readahead(data, window=8 * KB)

    f.seek(6 * KB)
    assert f.read(4 * KB) == data[6 * KB : 10 * KB]


def test_window_is_clamped_at_eof():
    data = _payload(10 * KB)
    f, raw = _readahead(data, window=8 * KB)

    f.seek(9 * KB)
    assert f.read(4 * KB) == data[9 * KB :]
    assert f.read(1) == b""
    assert raw.reads == [(9 * KB, 1 * KB)]


def test_seek_whence_and_tell():
    data = _payload(10 * KB)
    f, _ = _readahead(data, window=4 * KB)

    assert f.seek(-2, io.SEEK_END) == len(data) - 2
    assert f.read() == data[-2:]
    f.seek(100)
    f.seek(10, io.SEEK_CUR)
    assert f.tell() == 110


def test_window_fetch_stops_at_next_cached_window():
    data = _payload(64 * KB)
    f, raw = _readahead(data, window=16 * KB)

    f.seek(8 * KB)
    f.read(1 * KB)
    f.seek(0)
    assert f.read(2 * KB) == data[: 2 * KB]
    assert raw.reads == [(8 * KB, 16 * KB), (0, 8 * KB)]

    # A read that spans both cached windows is served without a new request.
    f.seek(7 * KB)
    assert f.read(2 * KB) == data[7 * KB : 9 * KB]
    assert len(raw.reads) == 2


def test_cached_bytes_are_bounded_by_budget():
    data = _payload(64 * KB)
    f, raw = _readahead(data, window=4 * KB, budget_windows=2)

    for off in (0, 16 * KB, 32 * KB):
        f.seek(off)
        f.read(1 * KB)
    assert len(f._windows) == 2
    assert f._cached <= 8 * KB

    # The oldest window (offset 0) was evicted, so reading it again issues a new request.
    f.seek(0)
    f.read(1 * KB)
    assert raw.reads[-1] == (0, 4 * KB)


def test_initial_fetch_is_capped_at_one_mib():
    data = bytes(8 * 1024 * KB)
    f, raw = _readahead(data, window=4 * 1024 * KB)

    f.read(128 * KB)
    assert raw.reads == [(0, 1024 * KB)]


def test_fetch_size_doubles_for_a_stream_read_to_the_end():
    data = bytes(16 * 1024 * KB)
    f, raw = _readahead(data, window=4 * 1024 * KB)

    while f.tell() < 7 * 1024 * KB:
        f.read(128 * KB)
    assert [n for _, n in raw.reads] == [1024 * KB, 2048 * KB, 4096 * KB]


def test_fetch_size_halves_after_a_window_is_evicted_unread():
    data = _payload(256 * KB)
    f, raw = _readahead(data, window=16 * KB, budget_windows=1)

    f.read(1 * KB)  # window A: [0, 16K)
    f.seek(128 * KB)
    f.read(1 * KB)  # window B evicts A, which was only read to 1K
    f.seek(1 * KB)
    assert f.read(1 * KB) == data[1 * KB : 2 * KB]
    assert raw.reads[-1] == (1 * KB, 8 * KB)


def _interleaved_streams(f, streams: int, stream_bytes: int, chunk: int) -> None:
    """Read `streams` sequential streams round-robin, like ORC reading the columns of a stripe."""
    for pos in range(0, stream_bytes, chunk):
        for s in range(streams):
            f.seek(s * stream_bytes + pos)
            f.read(chunk)


@pytest.mark.parametrize(
    ("budget_windows", "max_over_read", "max_request_fraction"),
    [(8, 1.35, 0.85), (32, 1.2, 0.3), (128, 1.01, 0.13)],
)
def test_many_interleaved_streams_bound_over_read(budget_windows, max_over_read, max_request_fraction):
    """64 equal streams read in lockstep, the worst case for a shared budget."""
    streams, stream_bytes, chunk = 64, 512 * KB, 8 * KB
    data = bytes(streams * stream_bytes)
    f, raw = _readahead(data, window=64 * KB, budget_windows=budget_windows)

    _interleaved_streams(f, streams, stream_bytes, chunk)

    assert sum(n for _, n in raw.reads) <= max_over_read * len(data)
    assert len(raw.reads) <= max_request_fraction * (len(data) // chunk)


def test_random_reads_match_source_bytes():
    data = _payload(200 * KB)
    f, _ = _readahead(data, window=16 * KB, budget_windows=3)
    rng = random.Random(0)
    for _ in range(500):
        off = rng.randrange(len(data))
        size = rng.randrange(1, 40 * KB)
        f.seek(off)
        assert f.read(size) == data[off : off + size]


# --- OrcReadAheadFileIO ----------------------------------------------------------------


def test_file_io_wraps_only_orc_files(tmp_path):
    orc_path = tmp_path / "a.orc"
    parquet_path = tmp_path / "a.parquet"
    orc.write_table(pa.table({"x": [1, 2, 3]}), str(orc_path))
    parquet_path.write_bytes(b"not orc")
    delegate = load_file_io(properties={}, location=str(orc_path))
    file_io = OrcReadAheadFileIO(delegate, window=1 * KB)

    assert isinstance(file_io.new_input(str(orc_path)).open(), pa.PythonFile)
    assert file_io.new_input(str(parquet_path)).open().read() == b"not orc"
    assert file_io.properties == delegate.properties


# --- DataLoaderSplit integration --------------------------------------------------------

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
)


def _table(n: int = 5000) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(n)], type=pa.string()),
            "value": pa.array([i * 0.5 for i in range(n)], type=pa.float64()),
        }
    )


def _with_readahead(split, window: int):
    ctx = split._scan_context
    split._scan_context = TableScanContext(
        table_metadata=ctx.table_metadata,
        io=ctx.io,
        projected_schema=ctx.projected_schema,
        table_id=ctx.table_id,
        orc_readahead_bytes=window,
    )
    return split


@pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC], ids=["parquet", "orc"])
@pytest.mark.parametrize("window", [64, 4 * KB, 8 * 1024 * KB])
def test_split_output_identical_with_readahead(tmp_path, file_format, window):
    table = _table()
    baseline = pa.Table.from_batches(list(_create_test_split(tmp_path, table, file_format, _SCHEMA)))
    split = _with_readahead(_create_test_split(tmp_path, table, file_format, _SCHEMA), window)
    result = pa.Table.from_batches(list(split))

    assert result.equals(baseline)
    assert result.num_rows == table.num_rows


def test_split_default_passes_original_io_to_arrow_scan(tmp_path):
    split = _create_test_split(tmp_path, _table(10), FileFormat.ORC, _SCHEMA)
    assert split._scan_context.orc_readahead_bytes == 0

    with patch("openhouse.dataloader.data_loader_split.ArrowScan", wraps=ArrowScan) as scan:
        list(split)
    assert scan.call_args.kwargs["io"] is split._scan_context.io


def test_split_with_readahead_wraps_io_and_keeps_scan_context_io(tmp_path):
    split = _with_readahead(_create_test_split(tmp_path, _table(10), FileFormat.ORC, _SCHEMA), 4 * KB)
    original_io = split._scan_context.io

    with patch("openhouse.dataloader.data_loader_split.ArrowScan", wraps=ArrowScan) as scan:
        list(split)
    io_arg = scan.call_args.kwargs["io"]
    assert isinstance(io_arg, OrcReadAheadFileIO)
    assert io_arg._delegate is original_io
    assert split._scan_context.io is original_io


def test_scan_context_pickle_round_trip_keeps_readahead(tmp_path):
    split = _with_readahead(_create_test_split(tmp_path, _table(10), FileFormat.ORC, _SCHEMA), 8 * 1024 * KB)
    restored = pickle.loads(pickle.dumps(split._scan_context))
    assert restored.orc_readahead_bytes == 8 * 1024 * KB


def test_scan_context_unpickle_without_readahead_arg_defaults_to_disabled(tmp_path):
    """Pickles produced before this field existed omit the trailing argument."""
    ctx = _create_test_split(tmp_path, _table(10), FileFormat.ORC, _SCHEMA)._scan_context
    _, args = ctx.__reduce__()
    restored = _unpickle_scan_context(*args[:-1])
    assert restored.orc_readahead_bytes == 0


# --- OpenHouseDataLoader ----------------------------------------------------------------


@pytest.mark.parametrize("context", [None, DataLoaderContext(), DataLoaderContext(orc_readahead_bytes=0)])
def test_loader_default_disables_readahead(tmp_path, context):
    loader = OpenHouseDataLoader(catalog=_make_real_catalog(tmp_path), database="db", table="tbl", context=context)
    assert all(split._scan_context.orc_readahead_bytes == 0 for split in loader)


def test_loader_passes_readahead_to_splits(tmp_path):
    loader = OpenHouseDataLoader(
        catalog=_make_real_catalog(tmp_path),
        database="db",
        table="tbl",
        context=DataLoaderContext(orc_readahead_bytes=8 * 1024 * KB),
    )
    splits = list(loader)
    assert splits
    assert all(split._scan_context.orc_readahead_bytes == 8 * 1024 * KB for split in splits)


def test_loader_rejects_negative_readahead(tmp_path):
    with pytest.raises(ValueError, match="orc_readahead_bytes"):
        OpenHouseDataLoader(
            catalog=_make_real_catalog(tmp_path),
            database="db",
            table="tbl",
            context=DataLoaderContext(orc_readahead_bytes=-1),
        )


def test_orc_reader_issues_fewer_requests_with_readahead(tmp_path):
    path = str(tmp_path / "multi_stripe.orc")
    orc.write_table(_table(200_000), path, stripe_size=64 * KB)
    delegate = load_file_io(properties={}, location=path)

    def count_preads(window: int) -> tuple[int, pa.Table]:
        calls = []
        original = _ReadAheadFile._pread

        def counting(self, offset, nbytes):
            calls.append(nbytes)
            return original(self, offset, nbytes)

        with (
            patch.object(_ReadAheadFile, "_pread", counting),
            OrcReadAheadFileIO(delegate, window=window).new_input(path).open() as f,
        ):
            result = orc.ORCFile(f).read()
        return len(calls), result

    unbuffered, expected = count_preads(1)  # window=1: every read passes through
    coalesced, result = count_preads(8 * 1024 * KB)

    assert result.equals(expected)
    assert unbuffered > 10
    assert coalesced < unbuffered / 5
