"""Helpers that only make sense on Databricks (lazy imports, so unit tests never need them)."""
from __future__ import annotations


def get_dbutils(spark=None):  # pragma: no cover - needs Databricks
    """Return ``dbutils``.

    Prefers ``databricks.sdk.runtime.dbutils`` or global ``builtins.dbutils`` because
    legacy ``pyspark.dbutils.DBUtils(spark)`` calls ``getNotebookArguments()`` via Py4J,
    which is rejected with Py4JSecurityException by ``PipelineRestrictedEntryPoint``
    inside Delta Live Tables / Lakeflow pipeline runtimes.
    """
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils
    except (ImportError, AttributeError):
        pass

    try:
        import builtins

        if hasattr(builtins, "dbutils"):
            return builtins.dbutils
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils

        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass

    raise RuntimeError("Unable to locate or initialize dbutils in the current runtime")
