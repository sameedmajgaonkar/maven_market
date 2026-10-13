"""Tests for the audit logging framework (maven_market.logging_utils)."""
import logging
from datetime import datetime

import pytest

from maven_market.logging_utils import AUDIT_SCHEMA_DDL, AuditLogger, AuditTableHandler


class Sink:
    def __init__(self):
        self.rows = []

    def __call__(self, table, row):
        self.rows.append((table, row))


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _logger(sink):
    return AuditLogger(table="maven_market_uc.audit.audit_logs", writer=sink)


# ---- row shape ----

def test_log_event_row_shape_and_timestamps():
    sink = Sink()
    _logger(sink).log_event(
        "bronze", "customers", {"rows_in": 10, "rows_out": 9, "rows_rejected": 1}, "SUCCESS"
    )
    table, row = sink.rows[0]
    assert table == "maven_market_uc.audit.audit_logs"
    assert row["stage"] == "bronze" and row["source"] == "customers"
    assert row["status"] == "SUCCESS" and row["error"] is None and row["traceback"] is None
    assert (row["rows_in"], row["rows_out"], row["rows_rejected"]) == (10, 9, 1)
    assert row["event_ts"] is not None and row["run_id"]


def test_row_records_start_and_end_times():
    sink = Sink()
    _logger(sink).log_event("bronze", "x", {}, "SUCCESS")
    row = sink.rows[0][1]
    assert isinstance(row["start_ts"], datetime) and isinstance(row["end_ts"], datetime)
    assert row["start_ts"] <= row["end_ts"] and row["event_ts"] == row["end_ts"]


def test_start_ts_is_derived_from_duration_when_not_given():
    sink = Sink()
    _logger(sink).log_event("bronze", "x", {}, "SUCCESS", duration_s=30.0)
    row = sink.rows[0][1]
    assert (row["end_ts"] - row["start_ts"]).total_seconds() == pytest.approx(30.0)


def test_missing_metrics_default_to_zero():
    sink = Sink()
    _logger(sink).log_event("silver", "dim_customer", {}, "SUCCESS")
    row = sink.rows[0][1]
    assert (row["rows_in"], row["rows_out"], row["rows_rejected"]) == (0, 0, 0)


def test_invalid_status_rejected():
    with pytest.raises(ValueError):
        _logger(Sink()).log_event("bronze", "x", {}, "WEIRD")


def test_audit_schema_ddl_matches_row_keys():
    # Spark can't infer a type from None (error is None on success), so the writer uses an
    # explicit schema; it must cover exactly the keys log_event emits.
    sink = Sink()
    row = _logger(sink).log_event("bronze", "x", {}, "SUCCESS")
    ddl_cols = [c.split()[0] for c in AUDIT_SCHEMA_DDL.split(",")]
    assert set(ddl_cols) == set(row)


# ---- built on the standard library `logging` ----

def test_uses_a_stdlib_logger_with_a_handler_that_writes_the_audit_table():
    audit = _logger(Sink())
    assert isinstance(audit.logger, logging.Logger)
    assert any(isinstance(h, AuditTableHandler) and isinstance(h, logging.Handler) for h in audit.logger.handlers)


def test_every_event_goes_through_the_logger_as_a_log_record():
    audit = _logger(Sink())
    seen = Capture()
    audit.logger.addHandler(seen)
    audit.log_event("bronze", "stores", {"rows_out": 3}, "SUCCESS")
    assert len(seen.records) == 1
    assert "bronze" in seen.records[0].getMessage() and "stores" in seen.records[0].getMessage()
    assert seen.records[0].audit_row["rows_out"] == 3


@pytest.mark.parametrize("status,level", [
    ("SUCCESS", logging.INFO), ("STARTED", logging.INFO), ("WARNING", logging.WARNING), ("FAILED", logging.ERROR),
])
def test_status_maps_to_a_log_level(status, level):
    audit = _logger(Sink())
    seen = Capture()
    audit.logger.addHandler(seen)
    row = audit.log_event("bronze", "x", {}, status)
    assert seen.records[0].levelno == level
    assert row["level"] == logging.getLevelName(level)


def test_each_event_is_written_exactly_once():
    sink = Sink()
    audit = _logger(sink)
    audit.log_event("bronze", "a", {}, "SUCCESS")
    audit.log_event("bronze", "b", {}, "SUCCESS")
    assert [r[1]["source"] for r in sink.rows] == ["a", "b"]


def test_events_are_also_printed_to_the_console(capsys):
    _logger(Sink()).log_event("bronze", "customers", {"rows_out": 7}, "SUCCESS")
    out = capsys.readouterr().out
    assert "bronze" in out and "customers" in out and "SUCCESS" in out


def test_loggers_do_not_leak_into_the_root_logger(caplog):
    with caplog.at_level(logging.DEBUG):
        _logger(Sink()).log_event("bronze", "x", {}, "SUCCESS")
    assert caplog.records == []


# ---- failures must be loud: stdlib handlers swallow emit() errors by default ----

def test_audit_write_failure_is_raised_not_swallowed():
    def broken_writer(table, row):
        raise RuntimeError("DELTA_METADATA_MISMATCH")

    with pytest.raises(RuntimeError, match="DELTA_METADATA_MISMATCH"):
        _logger(broken_writer).log_event("bronze", "x", {}, "SUCCESS")


# ---- track(): timing, success and failure with a real traceback ----

def test_track_logs_success_with_duration_and_ordered_timestamps():
    sink = Sink()
    with _logger(sink).track("bronze", "stores") as metrics:
        metrics["rows_in"] = 5
        metrics["rows_out"] = 5
    row = sink.rows[0][1]
    assert row["status"] == "SUCCESS" and row["rows_out"] == 5
    assert row["duration_s"] >= 0 and row["start_ts"] <= row["end_ts"]


def _fail_inside_a_function():
    raise RuntimeError("boom")


def test_track_failure_records_error_type_message_and_full_traceback_then_reraises():
    sink = Sink()
    with pytest.raises(RuntimeError):
        with _logger(sink).track("bronze", "stores"):
            _fail_inside_a_function()
    row = sink.rows[0][1]
    assert row["status"] == "FAILED" and row["level"] == "ERROR"
    assert row["error_type"] == "RuntimeError"
    assert row["error"] == "RuntimeError: boom" and "\n" not in row["error"]
    assert "Traceback (most recent call last)" in row["traceback"]
    assert "_fail_inside_a_function" in row["traceback"]   # the failing frame, not just the message
    assert row["start_ts"] <= row["end_ts"]


def test_track_still_logs_partial_metrics_on_failure():
    sink = Sink()
    with pytest.raises(ValueError):
        with _logger(sink).track("bronze", "x") as metrics:
            metrics["rows_in"] = 42
            raise ValueError("bad")
    assert sink.rows[0][1]["rows_in"] == 42
