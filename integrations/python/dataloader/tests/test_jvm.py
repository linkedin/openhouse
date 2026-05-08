import os

import pytest

from openhouse.dataloader._jvm import LIBHDFS_OPTS_ENV, apply_libhdfs_opts


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove LIBHDFS_OPTS before each test so tests are isolated."""
    monkeypatch.delenv(LIBHDFS_OPTS_ENV, raising=False)


def test_sets_env_when_unset() -> None:
    apply_libhdfs_opts("-Xmx512m")
    assert os.environ[LIBHDFS_OPTS_ENV] == "-Xmx512m"


def test_appends_to_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LIBHDFS_OPTS_ENV, "-Xmx256m")
    apply_libhdfs_opts("-verbose:gc")
    assert os.environ[LIBHDFS_OPTS_ENV] == "-Xmx256m -verbose:gc"


def test_skips_duplicate_args() -> None:
    apply_libhdfs_opts("-Xmx512m")
    apply_libhdfs_opts("-Xmx512m")
    assert os.environ[LIBHDFS_OPTS_ENV] == "-Xmx512m"
