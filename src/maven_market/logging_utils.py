"""Reusable audit logging built on the standard library ``logging`` module.

Every job/pipeline stage emits the same structured run record - start/end time, rows in/out/rejected,
status, error type/message and the **full traceback** - to two places:

* the console, through a normal ``logging.StreamHandler`` (so it shows up in the job's run output), and
* the audit Delta table (e.g. ``maven_market_uc.audit.audit_logs``) through ``AuditTableHandler``.

The table writer is injectable so the logic is unit-testable without Spark; the default writer appends
to a Delta table via the active SparkSession.

Each ``AuditLogger`` owns a standalone ``logging.Logger`` that does not propagate to the root logger, so
the audit trail is never duplicated or reconfigured by whatever else configures logging in the process.
"""
from __future__ import annotations

import logging
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

VALID_STATUSES = {"SUCCESS", "FAILED", "WARNING", "STARTED"}
_METRIC_KEYS = ("rows_in", "rows_out", "rows_rejected")
_LEVELS = {"SUCCESS": logging.INFO, "STARTED": logging.INFO, "WARNING": logging.WARNING, "FAILED": logging.ERROR}

Writer = Callable[[str, dict], None]

# Explicit schema: Spark cannot infer a column type from None (e.g. ``error`` on success rows).
# Must contain exactly the keys ``AuditLogger.log_event`` emits (a unit test enforces this).
AUDIT_SCHEMA_DDL = (
    "run_id STRING, event_ts TIMESTAMP, start_ts TIMESTAMP, end_ts TIMESTAMP, level STRING, "
    "stage STRING, source STRING, status STRING, error_type STRING, error STRING, traceback STRING, "
    "duration_s DOUBLE, rows_in LONG, rows_out LONG, rows_rejected LONG"
)


def _spark_writer(table: str, row: dict) -> None:  # pragma: no cover - needs Spark
    """Default writer: append one row to a Delta table using the active SparkSession.

    ``mergeSchema`` lets an audit table created by an older version of this module gain the newer
    columns (start/end time, traceback, ...) instead of failing with DELTA_METADATA_MISMATCH.
    """
    from pyspark.sql import SparkSession  # imported lazily; unavailable in plain unit tests

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    (spark.createDataFrame([row], schema=AUDIT_SCHEMA_DDL)
     .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table))


class AuditTableHandler(logging.Handler):
    """A ``logging`` handler that writes each audit record to the audit table.

    Standard handlers swallow errors in ``emit`` (they call ``handleError``, which just prints). An
    audit trail that silently loses rows is worse than a failing job, so errors from the writer are
    deliberately NOT caught here: a failed audit write fails the run, loudly.
    """

    def __init__(self, table: str, writer: Writer):
        super().__init__()
        self.table = table
        self._writer = writer

    def emit(self, record: logging.LogRecord) -> None:
        row = getattr(record, "audit_row", None)
        if row is not None:
            self._writer(self.table, row)


class AuditLogger:
    def __init__(self, table: str, writer: Writer | None = None, run_id: str | None = None):
        self.table = table
        self.run_id = run_id or uuid.uuid4().hex
        self._writer = writer or _spark_writer

        # A standalone Logger (not registered in the global hierarchy): no propagation, no clashes.
        self.logger = logging.Logger(f"maven_market.audit.{self.run_id}", level=logging.DEBUG)
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        self.logger.addHandler(console)
        self.logger.addHandler(AuditTableHandler(table, self._writer))

    def log_event(
        self,
        stage: str,
        source: str,
        metrics: dict | None,
        status: str,
        error: str | None = None,
        duration_s: float | None = None,
        start_ts: datetime | None = None,
        error_type: str | None = None,
        traceback_text: str | None = None,
        end_ts: datetime | None = None,
    ) -> dict:
        """Log one audit event through the logger (console + audit table) and return the row.

        ``end_ts`` defaults to now; pass it when recording something that already finished (e.g. a pipeline flow).
        """
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'; expected one of {sorted(VALID_STATUSES)}")
        metrics = metrics or {}
        level = _LEVELS[status]
        end_ts = end_ts or datetime.now(timezone.utc)
        if start_ts is None:
            start_ts = end_ts - timedelta(seconds=duration_s) if duration_s is not None else end_ts
        row = {
            "run_id": self.run_id,
            "event_ts": end_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "level": logging.getLevelName(level),
            "stage": stage,
            "source": source,
            "status": status,
            "error_type": error_type,
            "error": error,
            "traceback": traceback_text,
            "duration_s": duration_s,
            **{k: int(metrics.get(k, 0)) for k in _METRIC_KEYS},
        }
        message = (
            f"[{stage}] {source} {status} rows_in={row['rows_in']} rows_out={row['rows_out']} "
            f"rows_rejected={row['rows_rejected']}"
            + (f" duration={duration_s:.1f}s" if duration_s is not None else "")
            + (f" error={error}" if error else "")
        )
        self.logger.log(level, message, extra={"audit_row": row})
        return row

    @contextmanager
    def track(self, stage: str, source: str) -> Iterator[dict]:
        """Time a block; log SUCCESS on exit, or FAILED with the full traceback on error, then re-raise.

        Usage::

            with logger.track("bronze", "customers") as m:
                m["rows_in"] = ...
        """
        metrics: dict = {}
        started_at = datetime.now(timezone.utc)
        clock = time.monotonic()
        try:
            yield metrics
        except Exception as exc:
            self.log_event(
                stage, source, metrics, "FAILED",
                error="".join(traceback.format_exception_only(type(exc), exc)).strip(),
                duration_s=time.monotonic() - clock,
                start_ts=started_at,
                error_type=type(exc).__name__,
                traceback_text="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            )
            raise
        else:
            self.log_event(stage, source, metrics, "SUCCESS", duration_s=time.monotonic() - clock, start_ts=started_at)
