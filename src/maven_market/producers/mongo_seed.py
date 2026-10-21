"""Seed MongoDB Atlas with the master data (products + customers) from the source CSVs.

Which collections to load, their field types and their id field all come from ``mongo.collections``
in config; each collection is loaded from the CSV of the same name in ``producer.key_files``.
Idempotent: documents are upserted by the collection's id field, so re-running never duplicates.

    pip install pymongo
    export MONGO_URI='mongodb+srv://<user>:<password>@<cluster>.mongodb.net/'   # never commit this
    python -m maven_market.producers.mongo_seed [--collections products,customers] [--env dev]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from maven_market.ingestion.mongo_ingest import schema_types
from maven_market.producers.kafka_producer import _read_csv


def _parse_date(value: str) -> datetime:
    """Source CSVs use US ``M/D/YYYY``; stored in Mongo as a native BSON date."""
    return datetime.strptime(value, "%m/%d/%Y")


_CONVERTERS = {
    "INT": int, "LONG": int, "BIGINT": int,
    "DOUBLE": float, "FLOAT": float,
    "DATE": _parse_date,
    "STRING": str,
}


def _convert(value, type_: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return _CONVERTERS[type_](value)
    except KeyError:
        raise ValueError(f"Unsupported column type '{type_}' in mongo schema") from None


def build_documents(rows: list[dict], schema_ddl: str, id_field: str) -> list[dict]:
    """CSV rows -> typed Mongo documents following ``schema_ddl``.

    Blank cells become None; rows without an ``id_field`` value are skipped; ``_id`` is left to Mongo.
    """
    types = {c: t for c, t in schema_types(schema_ddl).items() if c != "_id"}
    docs = []
    for row in rows:
        doc = {column: _convert(row.get(column), type_) for column, type_ in types.items()}
        if doc.get(id_field) is None:
            continue
        docs.append(doc)
    return docs


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Atlas
    from pymongo import MongoClient, ReplaceOne

    from maven_market.config import get, load_config

    p = argparse.ArgumentParser(description="Seed Atlas with the master data")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument("--collections", default=None, type=lambda s: [x.strip() for x in s.split(",") if x.strip()])
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    uri = os.environ.get("MONGO_URI")
    if not uri:
        sys.exit("Set MONGO_URI first (export MONGO_URI='mongodb+srv://...'); it is not read from any file.")

    cfg = load_config(args.env, config_dir=args.config_dir)
    pcfg = cfg["producer"]
    available = get(cfg, "mongo.collections")
    names = args.collections or list(available)
    unknown = [n for n in names if n not in available]
    if unknown:
        sys.exit(f"Unknown collection(s) {unknown}; configured: {sorted(available)}")

    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    database = client[get(cfg, "mongo.database")]
    for name in names:
        spec = available[name]
        rows = _read_csv(Path(pcfg["keys_dir"]) / pcfg["key_files"][name])
        docs = build_documents(rows, spec["schema"], spec["id_field"])
        collection = database[name]
        result = collection.bulk_write(
            [ReplaceOne({spec["id_field"]: d[spec["id_field"]]}, d, upsert=True) for d in docs]
        )
        collection.create_index(spec["id_field"], unique=True)
        print(
            f"[mongo_seed] {name}: {len(docs)} documents -> {collection.full_name} "
            f"(inserted {result.upserted_count}, modified {result.modified_count}); "
            f"total now {collection.count_documents({})}"
        )
    client.close()


if __name__ == "__main__":  # pragma: no cover
    main()
