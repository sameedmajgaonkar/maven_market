"""Unit tests for databricks_utils helpers."""
import sys
from unittest.mock import MagicMock

import pytest

from maven_market.databricks_utils import get_dbutils


def test_get_dbutils_prefers_databricks_sdk_runtime(monkeypatch):
    mock_dbutils = MagicMock()
    mock_module = MagicMock()
    mock_module.dbutils = mock_dbutils

    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", mock_module)
    assert get_dbutils() is mock_dbutils


def test_get_dbutils_falls_back_to_builtins(monkeypatch):
    # simulate databricks.sdk.runtime missing
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)
    mock_dbutils = MagicMock()
    import builtins
    monkeypatch.setattr(builtins, "dbutils", mock_dbutils, raising=False)

    assert get_dbutils() is mock_dbutils


def test_get_dbutils_raises_when_all_fail(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", None)
    import builtins
    if hasattr(builtins, "dbutils"):
        monkeypatch.delattr(builtins, "dbutils")

    with pytest.raises(RuntimeError, match="Unable to locate or initialize dbutils"):
        get_dbutils(spark=None)
