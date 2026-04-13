"""Performance observability for the OpenHouse data loader.

Provides lightweight instrumentation to track time spent in each stage
and data volumes processed. Events are emitted through a pluggable
observer pattern with zero overhead when no observer is configured.
"""

from __future__ import annotations

import importlib
import logging
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

_perf_logger = logging.getLogger("openhouse.dataloader.perf")


@dataclass
class PerfEvent:
    """A performance measurement event.

    ``tags`` are low-cardinality dimensions for grouping/filtering (e.g. database,
    table, file_format).  ``metrics`` are measured values (e.g. row_count,
    batch_count, response_bytes).
    """

    operation: str
    duration_ms: float
    tags: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, int | float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"operation": self.operation, "duration_ms": self.duration_ms, **self.tags, **self.metrics}


@runtime_checkable
class PerfObserver(Protocol):
    """Protocol for receiving performance events."""

    def emit(self, event: PerfEvent) -> None: ...


class NullPerfObserver:
    """Default no-op observer with zero overhead."""

    def emit(self, event: PerfEvent) -> None:
        pass


class LoggingPerfObserver:
    """Observer that logs events to ``openhouse.dataloader.perf`` at DEBUG level."""

    def emit(self, event: PerfEvent) -> None:
        _perf_logger.debug("%s", event.to_dict())


class CompositeObserver:
    """Fans out events to multiple observers."""

    def __init__(self, *observers: PerfObserver) -> None:
        self._observers = observers

    def emit(self, event: PerfEvent) -> None:
        for observer in self._observers:
            observer.emit(event)


_observer: PerfObserver = NullPerfObserver()


def set_observer(obs: PerfObserver) -> None:
    """Set the performance observer for the data loader.

    Also forwards to ``pyiceberg.observability.set_observer()`` if available,
    so a single call enables both layers.
    """
    global _observer
    _observer = obs

    try:
        from pyiceberg.observability import set_observer as pyiceberg_set_observer

        pyiceberg_set_observer(obs)
    except (ImportError, ModuleNotFoundError):
        pass


def get_observer() -> PerfObserver:
    """Return the current performance observer."""
    return _observer


@dataclass(frozen=True)
class PerfConfig:
    """Serializable perf configuration that travels with splits to workers.

    Attributes:
        tags: Session-level tags injected into every performance event
            (e.g. cluster, tenant).  Per-event tags override these.
        observer_type: Observer to bootstrap on workers.  ``"logging"``
            (default) creates a :class:`LoggingPerfObserver`, ``"null"``
            creates a :class:`NullPerfObserver`, and a dotted import path
            (e.g. ``"mypackage.KafkaObserver"``) dynamically imports and
            instantiates the class.
        observer_kwargs: Constructor keyword arguments forwarded to the
            observer created by *observer_type*.
    """

    tags: Mapping[str, str] = field(default_factory=dict)
    observer_type: str = "logging"
    observer_kwargs: Mapping[str, Any] = field(default_factory=dict)


class EnrichingObserver:
    """Wraps an observer and prepends session-level tags to every event.

    Per-event tags take precedence over session tags.
    """

    def __init__(self, inner: PerfObserver, session_tags: Mapping[str, str]) -> None:
        self._inner = inner
        self._session_tags = session_tags

    def emit(self, event: PerfEvent) -> None:
        event.tags = {**self._session_tags, **event.tags}
        self._inner.emit(event)


def _create_observer(observer_type: str, observer_kwargs: Mapping[str, Any] | None = None) -> PerfObserver:
    """Create an observer instance from a type descriptor and kwargs."""
    kwargs = dict(observer_kwargs) if observer_kwargs else {}
    if observer_type == "logging":
        return LoggingPerfObserver(**kwargs)
    if observer_type == "null":
        return NullPerfObserver()
    module_path, class_name = observer_type.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    observer: PerfObserver = cls(**kwargs)
    return observer


def bootstrap_observer(config: PerfConfig) -> None:
    """Idempotent worker-side observer setup from serialized config.

    If an observer other than :class:`NullPerfObserver` is already set
    (e.g. the planner process already configured one), this is a no-op.
    """
    if not isinstance(get_observer(), NullPerfObserver):
        return
    observer = _create_observer(config.observer_type, config.observer_kwargs)
    if config.tags:
        observer = EnrichingObserver(observer, config.tags)
    set_observer(observer)


class _PerfTimerContext:
    """Mutable context returned by ``perf_timer``.

    Use ``.tag()`` for dimensions (low-cardinality strings) and
    ``.metric()`` for measured values (counts, sizes, etc.).
    """

    def __init__(self) -> None:
        self.tags: dict[str, str] = {}
        self.metrics: dict[str, int | float] = {}

    def tag(self, key: str, value: str) -> None:
        """Set a dimension tag on the performance event."""
        self.tags[key] = value

    def metric(self, key: str, value: int | float) -> None:
        """Set a metric value on the performance event."""
        self.metrics[key] = value


@contextmanager
def perf_timer(operation: str, **tags: str) -> Iterator[_PerfTimerContext]:
    """Context manager that measures elapsed time and emits a ``PerfEvent``.

    Keyword arguments are recorded as dimension tags.  Use ``ctx.metric()``
    inside the block for measured values.

    Usage::

        with perf_timer("dataloader.iter") as ctx:
            ctx.metric("split_count", n)
            ...
    """
    ctx = _PerfTimerContext()
    ctx.tags.update(tags)
    start = time.monotonic()
    try:
        yield ctx
    finally:
        duration_ms = (time.monotonic() - start) * 1000
        _observer.emit(PerfEvent(operation=operation, duration_ms=duration_ms, tags=ctx.tags, metrics=ctx.metrics))
