import os

import pytest

from openhouse.dataloader._jvm import apply_libhdfs_opts


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove LIBHDFS_OPTS before each test so tests are isolated."""
    monkeypatch.delenv("LIBHDFS_OPTS", raising=False)


def test_sets_env_when_unset() -> None:
    apply_libhdfs_opts("-Xmx512m")
    assert os.environ["LIBHDFS_OPTS"] == "-Xmx512m"


def test_appends_to_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIBHDFS_OPTS", "-Xmx256m")
    apply_libhdfs_opts("-verbose:gc")
    assert os.environ["LIBHDFS_OPTS"] == "-Xmx256m -verbose:gc"
