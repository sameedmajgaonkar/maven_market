"""Tests for turning a pipeline event log into audit rows (pure Python; the Spark read is a thin wrapper)."""
import json
from datetime import datetime, timedelta

import pytest

from maven_market.jobs.pipeline_audit import latest_update_id, parse_args, summarize_update

T0 = datetime(2026, 10, 14, 18, 0, 0)


def _event(seconds, event_type, update_id="u1", flow=None, details=None, error=None):
    return {
        "timestamp": T0 + timedelta(seconds=seconds), "event_type": event_type, "update_id": update_id,
        "flow_name": flow, "details": json.dumps(details) if details is not None else None,
        "error": json.dumps(error) if error is not None else None,
    }


def _flow(seconds, flow, status, **extra):
    return _event(seconds, "flow_progress", flow=flow, details={"flow_progress": {"status": status, **extra}})


def _update(seconds, state, error=None):
    return _event(seconds, "update_progress", details={"update_progress": {"state": state}}, error=error)


def test_parse_args_requires_a_pipeline_id():
    with pytest.raises(SystemExit):
        parse_args([])
    assert parse_args(["--pipeline-id", "p1"]).pipeline_id == "p1"


def test_latest_update_id_is_the_most_recent_create_update():
    events = [_event(0, "create_update", update_id="old"), _event(100, "create_update", update_id="new")]
    assert latest_update_id(events) == "new"
    assert latest_update_id([]) is None


def test_a_successful_flow_reports_rows_out_and_rejected_records():
    events = [
        _flow(1, "maven_market_uc.silver.transactions", "RUNNING"),
        _flow(9, "maven_market_uc.silver.transactions", "COMPLETED",
              metrics={"num_output_rows": 90}, data_quality={"dropped_records": 10, "warned_records": 0}),
        _update(10, "COMPLETED"),
    ]
    flow, update = summarize_update(events, "u1")
    assert (flow["stage"], flow["source"], flow["status"]) == ("silver", "transactions", "SUCCESS")
    assert flow["metrics"] == {"rows_in": 100, "rows_out": 90, "rows_rejected": 10}
    assert (flow["end_ts"] - flow["start_ts"]).total_seconds() == 8
    assert (update["stage"], update["source"], update["status"]) == ("pipeline", "update", "SUCCESS")
    assert update["metrics"]["rows_out"] == 90


def test_flows_without_metrics_report_zero_rows():
    events = [_flow(1, "maven_market_uc.bronze.stores", "COMPLETED")]
    (flow,) = summarize_update(events, "u1")
    assert flow["metrics"] == {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}


def test_a_failed_flow_and_update_carry_the_error():
    error = {"exceptions": [{"class_name": "org.apache.spark.SparkException", "message": "boom"}]}
    events = [
        _flow(1, "maven_market_uc.bronze.orders", "RUNNING"),
        _event(5, "flow_progress", flow="maven_market_uc.bronze.orders",
               details={"flow_progress": {"status": "FAILED"}}, error=error),
        _update(6, "FAILED", error=error),
    ]
    flow, update = summarize_update(events, "u1")
    assert flow["status"] == "FAILED" and flow["error"] == "boom"
    assert flow["error_type"] == "org.apache.spark.SparkException"
    assert update["status"] == "FAILED" and update["error"] == "boom"


def test_only_the_requested_update_is_summarised():
    events = [
        _flow(1, "maven_market_uc.gold.fact_sales", "COMPLETED", metrics={"num_output_rows": 5}),
        {**_flow(2, "maven_market_uc.gold.fact_sales", "COMPLETED", metrics={"num_output_rows": 7}), "update_id": "u2"},
    ]
    (flow,) = summarize_update(events, "u2")
    assert flow["metrics"]["rows_out"] == 7


def test_internal_flows_without_a_schema_are_recorded_under_the_pipeline_stage():
    (flow,) = summarize_update([_flow(1, "raw_orders_stream", "COMPLETED")], "u1")
    assert (flow["stage"], flow["source"]) == ("pipeline", "raw_orders_stream")


def test_unfinished_flows_are_not_reported():
    assert summarize_update([_flow(1, "maven_market_uc.gold.fact_sales", "RUNNING")], "u1") == []
