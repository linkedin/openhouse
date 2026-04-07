"""JVM configuration utilities for the HDFS client."""

import os
import threading

LIBHDFS_OPTS_ENV = "LIBHDFS_OPTS"
"""Environment variable read by libhdfs when starting the JNI JVM."""

_lock = threading.Lock()


def apply_libhdfs_opts(jvm_args: str) -> None:
    """Merge *jvm_args* into the JNI JVM options environment variable.

    Appends to any existing value.  Must be called before the first
    HDFS access in the current process (the JVM is started once and
    reads these options only at startup).  Thread-safe and idempotent —
    duplicate args are not appended.
    """
    with _lock:
        existing = os.environ.get(LIBHDFS_OPTS_ENV, "")
        if jvm_args in existing:
            return
        merged = f"{existing} {jvm_args}".strip() if existing else jvm_args
        os.environ[LIBHDFS_OPTS_ENV] = merged
