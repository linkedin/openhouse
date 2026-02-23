from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def log_duration(logger: logging.Logger, message: str, *args: object) -> Iterator[None]:
    """Context manager that logs elapsed time at INFO level.

    Appends " in %.3fs" to the message and logs at INFO on success.
    If the block raises an exception, nothing is logged.

    Example::

        with log_duration(logger, "Loaded table %s", table_name):
            table = catalog.load_table(table_name)
        # logs: "Loaded table my_table in 0.123s"
    """
    start = time.monotonic()
    yield
    elapsed = time.monotonic() - start
    logger.info(f"{message} in %.3fs", *args, elapsed)
