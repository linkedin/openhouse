"""Bounded, adaptive read-ahead for ORC data files.

PyIceberg opens data files with ``FileSystem.open_input_file`` and, for ORC, drops the
``pre_buffer``/``buffer_size`` options it uses for Parquet. Arrow's ORC adapter then asks
for about 128 KB per ``ReadAt``, and on ``HadoopFileSystem`` every ``ReadAt`` is a
separate synchronous HDFS pread. Each pread pays a full network round trip, so reading a
file from another data center turns into thousands of ~50 ms requests.

``OrcReadAheadFileIO`` wraps a FileIO so that ORC files are opened through a file object
that turns a read miss into one larger pread and serves later reads from the fetched
window. ORC reads each column stream sequentially, but round-robins across all streams of
a stripe (and Arrow overlaps the next stripe), so a wide file has hundreds of streams in
flight. A fixed large window per miss would evict most windows before they are read. The
fetch size is therefore learned per stream: it starts at ``min(window, 1 MiB)``, halves
when a window is evicted before it is read to the end, and doubles when a window is read
to the end while no other window was evicted unread.
Cached bytes per open file are bounded by ``window * budget_windows``.
"""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from typing import Any, cast

import pyarrow as pa
from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile

DEFAULT_BUDGET_WINDOWS = 32
_INITIAL_FETCH = 1024 * 1024
_MAX_STREAMS = 8192


class _Window:
    __slots__ = ("buf", "read_end", "fetch", "evictions")

    def __init__(self, buf: bytes, read_end: int, fetch: int, evictions: int):
        self.buf = buf
        self.read_end = read_end  # file offset up to which this window has been read
        self.fetch = fetch  # fetch size that produced it, used to size the stream's next fetch
        self.evictions = evictions  # unread evictions so far when it was fetched


class _ReadAheadFile:
    """Seekable raw file for ``pyarrow.PythonFile`` that coalesces small positional reads."""

    def __init__(self, stream: Any, size: int, window: int, budget_windows: int = DEFAULT_BUDGET_WINDOWS):
        self._stream = stream
        self._size = size
        self._window = window
        self._budget = window * max(1, budget_windows)
        self._cached = 0
        self._windows: OrderedDict[int, _Window] = OrderedDict()
        # Next fetch size for a stream, keyed by the offset where the stream continues.
        self._next_fetch: OrderedDict[int, int] = OrderedDict()
        self._evictions = 0  # windows evicted before being read to the end
        self._pos = 0
        self._closed = False
        self._lock = threading.Lock()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._windows.clear()
            self._next_fetch.clear()
            self._stream.close()

    def size(self) -> int:
        return self._size

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            self._pos = offset
        elif whence == os.SEEK_CUR:
            self._pos += offset
        elif whence == os.SEEK_END:
            self._pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self._pos

    def _pread(self, offset: int, nbytes: int) -> bytes:
        if hasattr(self._stream, "read_at"):
            return cast(bytes, self._stream.read_at(nbytes, offset))
        self._stream.seek(offset)
        return cast(bytes, self._stream.read(nbytes))

    def _retire(self, start: int) -> None:
        w = self._windows.pop(start)
        self._cached -= len(w.buf)
        end = start + len(w.buf)
        if w.read_end < end:
            self._evictions += 1
            key, fetch = w.read_end, max(1, w.fetch // 2)
        elif w.evictions == self._evictions:
            key, fetch = end, min(self._window, w.fetch * 2)
        else:
            # Another window was evicted unread while this one was cached: the budget is full, so don't grow.
            key, fetch = end, w.fetch
        self._next_fetch[key] = fetch
        self._next_fetch.move_to_end(key)
        while len(self._next_fetch) > _MAX_STREAMS:
            self._next_fetch.popitem(last=False)

    def _read_range(self, offset: int, nbytes: int) -> bytes | memoryview:
        if nbytes >= self._window:
            return self._pread(offset, nbytes)
        for start, w in self._windows.items():
            end = start + len(w.buf)
            if start <= offset < end:
                self._windows.move_to_end(start)
                head = memoryview(w.buf)[offset - start : offset - start + nbytes]
                w.read_end = max(w.read_end, offset + len(head))
                if w.read_end >= end:
                    self._retire(start)
                if len(head) == nbytes:
                    return head
                # The read runs past this window: serve the rest from the next one.
                return bytes(head) + bytes(self._read_range(offset + len(head), nbytes - len(head)))
        fetch = self._next_fetch.pop(offset, min(self._window, _INITIAL_FETCH))
        end = min(offset + max(nbytes, fetch), self._size)
        # Stop the fetch where the next cached window begins, so cached bytes are not read twice.
        for start in self._windows:
            if offset + nbytes <= start < end:
                end = start
        buf = self._pread(offset, end - offset)
        if len(buf) > nbytes:
            self._windows[offset] = _Window(buf, offset + nbytes, fetch, self._evictions)
            self._cached += len(buf)
            while self._cached > self._budget:
                self._retire(next(iter(self._windows)))
        else:
            # Nothing was read ahead; keep the stream's size instead of growing it back.
            self._next_fetch[offset + len(buf)] = fetch
        return memoryview(buf)[:nbytes]

    def read(self, nbytes: int | None = None) -> bytes | memoryview:
        with self._lock:
            remaining = self._size - self._pos
            if nbytes is None or nbytes < 0 or nbytes > remaining:
                nbytes = max(0, remaining)
            if nbytes == 0:
                return b""
            data = self._read_range(self._pos, nbytes)
            self._pos += len(data)
            return data


class _OrcReadAheadInputFile(InputFile):
    def __init__(self, delegate: InputFile, window: int, budget_windows: int):
        super().__init__(location=delegate.location)
        self._delegate = delegate
        self._window = window
        self._budget_windows = budget_windows

    def __len__(self) -> int:
        return len(self._delegate)

    def exists(self) -> bool:
        return self._delegate.exists()

    def open(self, seekable: bool = True) -> InputStream:
        stream = self._delegate.open(seekable=seekable)
        if not seekable:
            return stream
        size = stream.size() if hasattr(stream, "size") else len(self._delegate)
        raw = _ReadAheadFile(stream, size, self._window, self._budget_windows)
        return pa.PythonFile(raw, mode="r")  # type: ignore[return-value,no-any-return]


class OrcReadAheadFileIO(FileIO):
    """FileIO that applies bounded read-ahead to ``.orc`` files and delegates everything else."""

    def __init__(self, delegate: FileIO, window: int, budget_windows: int = DEFAULT_BUDGET_WINDOWS):
        if window <= 0:
            raise ValueError(f"window must be positive, got {window}")
        super().__init__(properties=delegate.properties)
        self._delegate = delegate
        self._window = window
        self._budget_windows = budget_windows

    def new_input(self, location: str) -> InputFile:
        input_file = self._delegate.new_input(location)
        if location.lower().endswith(".orc"):
            return _OrcReadAheadInputFile(input_file, self._window, self._budget_windows)
        return input_file

    def new_output(self, location: str) -> OutputFile:
        return self._delegate.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        self._delegate.delete(location)
