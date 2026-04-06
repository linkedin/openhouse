"""JVM configuration utilities for the HDFS client."""

import os

LIBHDFS_OPTS_ENV = "LIBHDFS_OPTS"
"""Environment variable read by libhdfs when starting the JNI JVM."""


def apply_libhdfs_opts(jvm_args: str) -> None:
    """Merge *jvm_args* into the JNI JVM options environment variable.

    Appends to any existing value.  Must be called before the first
    HDFS access in the current process (the JVM is started once and
    reads these options only at startup).
    """
    existing = os.environ.get(LIBHDFS_OPTS_ENV, "")
    merged = f"{existing} {jvm_args}".strip() if existing else jvm_args
    os.environ[LIBHDFS_OPTS_ENV] = merged
