"""Job entry point: record the latest pipeline update in the audit table.

The Bronze/Silver/Gold pipeline cannot log from inside its dataset functions (they may only *define*
datasets), so this task runs after every update and turns the pipeline's own event log into the same
structured audit rows every other stage writes: one row per flow with rows out, rows rejected by
expectations, duration and (for a failure) the error, plus one row for the update as a whole. The audit
``run_id`` is the pipeline update id, so a row can be traced back to the update in the pipeline UI.

Run as a Databricks python_wheel_task (``pipeline_audit`` console script) with ``run_if: ALL_DONE`` so a
failed update is recorded too.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from maven_market.config import audit_table, load_config
from maven_market.logging_utils import AuditLogger

_FLOW_STATUS = {
    "COMPLETED": "SUCCESS", "IDLE": "SUCCESS",
    "FAILED": "FAILED",
    "STOPPED": "WARNING", "SKIPPED": "WARNING", "EXCLUDED": "WARNING",
}
_UPDATE_STATUS = {"COMPLETED": "SUCCESS", "FAILED": "FAILED", "CANCELED": "WARNING"}


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Write the latest pipeline update to the audit table")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument("--pipeline-id", required=True)
    return p.parse_args(argv)


def _json(text) -> dict:
    if not text:
        return {}
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _error_fields(error_json) -> tuple[str | None, str | None]:
    """``(error_type, message)`` from an event-log ``error`` struct serialised as JSON."""
    exceptions = _json(error_json).get("exceptions") or []
    if not exceptions:
        return None, None
    first = exceptions[0]
    return first.get("class_name"), first.get("message")


def _split_flow(flow_name: str) -> tuple[str, str]:
    """``catalog.schema.table`` -> ``(schema, table)``; anything else is a pipeline-internal flow."""
    parts = flow_name.split(".")
    return (parts[-2], parts[-1]) if len(parts) >= 3 else ("pipeline", flow_name)


def latest_update_id(events: list[dict]) -> str | None:
    created = [e for e in events if e["event_type"] == "create_update"]
    return max(created, key=lambda e: e["timestamp"])["update_id"] if created else None


def summarize_update(events: list[dict], update_id: str) -> list[dict]:
    """Audit rows (``stage, source, status, metrics, ...``) for one pipeline update, flows first.

    ``events`` are event-log rows with ``timestamp`` (datetime), ``event_type``, ``flow_name``,
    ``update_id``, ``details`` and ``error`` (both JSON text).
    """
    mine = sorted((e for e in events if e["update_id"] == update_id), key=lambda e: e["timestamp"])
    rows: list[dict] = []

    by_flow: dict[str, list[dict]] = {}
    for e in mine:
        if e["event_type"] == "flow_progress" and e["flow_name"]:
            by_flow.setdefault(e["flow_name"], []).append(e)
    for flow, flow_events in by_flow.items():
        statuses = [(e, _json(e["details"]).get("flow_progress", {})) for e in flow_events]
        terminal = [(e, fp) for e, fp in statuses if fp.get("status") in _FLOW_STATUS]
        if not terminal:
            continue
        last_event, last = terminal[-1]
        measured = [fp for _, fp in statuses if "num_output_rows" in (fp.get("metrics") or {})]
        quality = [fp.get("data_quality") for _, fp in statuses if fp.get("data_quality")]
        rows_out = int((measured[-1]["metrics"] if measured else {}).get("num_output_rows", 0))
        rejected = int((quality[-1] if quality else {}).get("dropped_records", 0) or 0)
        error_type, error = _error_fields(last_event["error"])
        stage, source = _split_flow(flow)
        rows.append({
            "stage": stage, "source": source, "status": _FLOW_STATUS[last["status"]],
            "metrics": {"rows_in": rows_out + rejected, "rows_out": rows_out, "rows_rejected": rejected},
            "start_ts": flow_events[0]["timestamp"], "end_ts": last_event["timestamp"],
            "error": error, "error_type": error_type,
        })

    updates = [e for e in mine if e["event_type"] == "update_progress"]
    terminal = [e for e in updates if _json(e["details"]).get("update_progress", {}).get("state") in _UPDATE_STATUS]
    if terminal:
        final = terminal[-1]
        state = _json(final["details"])["update_progress"]["state"]
        error_type, error = _error_fields(final["error"])
        rows.append({
            "stage": "pipeline", "source": "update", "status": _UPDATE_STATUS[state],
            "metrics": {"rows_in": sum(r["metrics"]["rows_in"] for r in rows),
                        "rows_out": sum(r["metrics"]["rows_out"] for r in rows),
                        "rows_rejected": sum(r["metrics"]["rows_rejected"] for r in rows)},
            "start_ts": mine[0]["timestamp"], "end_ts": final["timestamp"],
            "error": error, "error_type": error_type,
        })
    return rows


def _duration(row: dict) -> float:
    return max((row["end_ts"] - row["start_ts"]).total_seconds(), 0.0)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Databricks
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    events = [
        {**r.asDict(), "timestamp": r["timestamp"]}
        for r in spark.sql(
            "SELECT timestamp, event_type, origin.flow_name AS flow_name, origin.update_id AS update_id, "
            "details, to_json(error) AS error "
            f"FROM event_log('{args.pipeline_id}') "
            "WHERE event_type IN ('create_update', 'update_progress', 'flow_progress')"
        ).collect()
    ]
    update_id = latest_update_id(events)
    if update_id is None:
        raise RuntimeError(f"No pipeline update found in the event log of {args.pipeline_id}")

    audit = AuditLogger(table=audit_table(cfg), run_id=update_id)
    rows = summarize_update(events, update_id)
    for row in rows:
        start = row["start_ts"] if isinstance(row["start_ts"], datetime) else None
        audit.log_event(
            row["stage"], row["source"], row["metrics"], row["status"], error=row["error"],
            duration_s=_duration(row), start_ts=start, end_ts=row["end_ts"], error_type=row["error_type"],
        )
    print(f"[pipeline_audit] recorded {len(rows)} rows for update {update_id}")


if __name__ == "__main__":  # pragma: no cover
    main()
