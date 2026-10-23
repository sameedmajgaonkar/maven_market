"""MongoDB Atlas -> Bronze: pure helpers (no Spark) used by the Bronze streaming source.

Bronze reads MongoDB through a Spark *Python streaming data source* (``mongo_source.py``) declared inside the
Lakeflow pipeline. This module holds the logic that source needs, kept free of Spark so it is unit-tested:

* ``documents_to_rows`` projects BSON documents onto the configured Spark schema (BSON dates, Decimal128,
  nested documents, missing fields and wrong types all become safe values).
* ``snapshot_digest`` / ``plan_snapshot`` implement *snapshot-on-change*: the stream offset is a digest of
  the whole collection, and a micro-batch is emitted only when that digest differs from the offset the
  stream already committed. An unchanged collection appends nothing; a changed one appends a fresh snapshot
  of the (bounded, master-data sized) collection, and Silver's SCD2 flow turns the real differences into
  history (``_ingested_at`` is excluded from change tracking, so unchanged rows do not create versions).

This is deliberately *not* CDC: deletes are not detected and a single edit re-appends the snapshot.

The connection URI comes from the secret scope; it is never in config or code.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime


def schema_types(ddl: str) -> dict[str, str]:
    """``{column: TYPE}`` from a ``"a INT, b STRING"`` DDL string (simple, non-parameterised types)."""
    out = {}
    for part in ddl.split(","):
        if part.strip():
            name, type_ = part.split()[:2]
            out[name] = type_.upper()
    return out


def schema_columns(ddl: str) -> list[str]:
    """Column names from a ``"a INT, b STRING"`` DDL string, in order."""
    return list(schema_types(ddl))


def _as_date(value):
    """datetime -> date; ISO string -> date; date/None unchanged."""
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _normalize_value(value, type_: str | None):
    """Coerce a BSON / Python value into the Spark type named by ``type_`` (``None`` on a failed cast)."""
    if value is None:
        return None
    if hasattr(value, "to_decimal"):  # bson.Decimal128
        value = float(value.to_decimal())
    if type_ == "DATE":
        return _as_date(value)
    if type_ in ("INT", "LONG", "BIGINT"):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    if type_ in ("DOUBLE", "FLOAT"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    if type_ == "BOOLEAN":
        if isinstance(value, str):
            return value.lower() in ("true", "1", "t", "y", "yes")
        return bool(value)
    if type_ == "STRING":
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str, sort_keys=True)
        return str(value)
    return value


def documents_to_rows(docs, columns: list[str], types: dict[str, str] | None = None) -> list[dict]:
    """Project Mongo documents onto the configured columns.

    ``_id`` (an ObjectId) becomes a string; fields missing from a document become NULL; fields not in
    ``columns`` are dropped, so an unexpected new field cannot break the load; every value is coerced to
    its declared type.
    """
    types = types or {}
    rows = []
    for doc in docs:
        row = {}
        for column in columns:
            raw = doc.get(column)
            row[column] = str(raw) if column == "_id" and raw is not None else _normalize_value(raw, types.get(column))
        rows.append(row)
    return rows


def snapshot_digest(rows: list[dict], columns: list[str], id_field: str) -> str:
    """Stable digest of a snapshot: independent of document order, sensitive to any value change."""
    ordered = sorted(rows, key=lambda r: str(r.get(id_field)))
    payload = json.dumps([[r.get(c) for c in columns] for r in ordered], default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def plan_snapshot(rows: list[dict], columns: list[str], id_field: str, committed_digest: str | None):
    """Decide what a micro-batch emits: ``(rows_to_emit, end_digest)``.

    Nothing when the collection is unchanged since ``committed_digest`` (the offset the stream already
    committed); otherwise the whole snapshot, with its digest as the new offset.
    """
    if not rows:
        raise ValueError("MongoDB collection returned no documents; refusing to continue")
    digest = snapshot_digest(rows, columns, id_field)
    if digest == committed_digest:
        return [], digest
    return rows, digest


def fetch_documents(uri: str, database: str, collection: str) -> list[dict]:  # pragma: no cover - needs Atlas
    """Read every document of a (bounded, master-data sized) collection, with explicit timeouts."""
    from pymongo import MongoClient  # lazy: only needed on the machine that talks to Atlas

    client = MongoClient(uri, serverSelectionTimeoutMS=20000, connectTimeoutMS=15000, socketTimeoutMS=30000)
    try:
        return list(client[database][collection].find({}))
    finally:
        client.close()
