from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def log_duration(logger: logging.Logger, message: str, *args: object) -> Iterator[None]:
    """Context manager that logs elapsed time.

    Logs at INFO on success and WARNING on failure, both with elapsed time.
    """
    start = time.monotonic()
    try:
        yield
    except Exception:
        elapsed = time.monotonic() - start
        logger.warning(f"{message} failed after %.3fs", *args, elapsed)
        raise
    else:
        elapsed = time.monotonic() - start
        logger.info(f"{message} in %.3fs", *args, elapsed)
