"""Event simulator: emits Orders and Inventory events to Confluent Kafka.

Orders/Inventory are not in the source dataset, so we simulate them. Crucially the keys
(customer_id / product_id / store_id) are sampled from the REAL dimension CSVs, so every event
joins to a dimension in Silver (the reason a random generator such as Datagen was rejected).

Layout: pure, unit-tested event builders + a thin Kafka runner (``run``) that needs
``confluent_kafka`` and credentials from env vars.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator


@dataclass(frozen=True)
class EventKeys:
    customer_ids: list[int]
    products: dict[int, float]  # product_id -> retail price
    store_ids: list[int]


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def load_event_keys(keys_dir: str | Path, key_files: dict[str, str]) -> EventKeys:
    """Read the real customer/product/store ids from the dimension CSVs."""
    base = Path(keys_dir)
    customers = [int(r["customer_id"]) for r in _read_csv(base / key_files["customers"])]
    products = {
        int(r["product_id"]): float(r["product_retail_price"])
        for r in _read_csv(base / key_files["products"])
    }
    stores = [int(r["store_id"]) for r in _read_csv(base / key_files["stores"])]
    for name, values in (("customers", customers), ("products", products), ("stores", stores)):
        if not values:
            raise ValueError(f"No '{name}' keys found in {base}; cannot simulate events")
    return EventKeys(customer_ids=customers, products=products, store_ids=stores)


def build_order_event(keys: EventKeys, rng: random.Random, now: datetime) -> dict:
    product_id = rng.choice(list(keys.products))
    return {
        "event_type": "order",
        "order_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "customer_id": rng.choice(keys.customer_ids),
        "product_id": product_id,
        "store_id": rng.choice(keys.store_ids),
        "quantity": rng.randint(1, 5),
        "unit_price": keys.products[product_id],
        "order_ts": now.isoformat(),
    }


def build_inventory_event(keys: EventKeys, rng: random.Random, now: datetime) -> dict:
    return {
        "event_type": "inventory",
        "product_id": rng.choice(list(keys.products)),
        "store_id": rng.choice(keys.store_ids),
        "quantity_on_hand": rng.randint(0, 500),
        "event_ts": now.isoformat(),
    }


def iter_events(
    keys: EventKeys,
    seed: int = 42,
    max_events: int = 0,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    orders_ratio: float = 0.7,
) -> Iterator[tuple[str, dict]]:
    """Yield ``(topic, event)`` pairs. ``max_events=0`` means run forever."""
    rng = random.Random(seed)
    emitted = 0
    while max_events == 0 or emitted < max_events:
        if rng.random() < orders_ratio:
            yield "orders", build_order_event(keys, rng, now_fn())
        else:
            yield "inventory", build_inventory_event(keys, rng, now_fn())
        emitted += 1


def serialize(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def keys_from_rows(customer_ids, product_rows, store_ids) -> EventKeys:
    """Build EventKeys from already-loaded rows (used when keys come from Bronze tables).

    ``product_rows`` is an iterable of ``(product_id, retail_price)``; products with no price are
    skipped since an order needs a unit price.
    """
    customers = [int(c) for c in customer_ids]
    products = {int(pid): float(price) for pid, price in product_rows if price is not None}
    stores = [int(s) for s in store_ids]
    for name, values in (("customers", customers), ("products", products), ("stores", stores)):
        if not values:
            raise ValueError(f"No '{name}' keys available; cannot simulate events")
    return EventKeys(customer_ids=customers, products=products, store_ids=stores)


def parse_job_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate orders + inventory events into Kafka")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument("--max-events", type=int, default=None, help="Override producer.max_events")
    p.add_argument("--events-per-second", type=float, default=None, help="Override producer.events_per_second")
    return p.parse_args(argv)


def load_keys_from_bronze(spark, cfg: dict) -> EventKeys:  # pragma: no cover - needs Spark
    """Read the real keys from the Bronze dimension tables (the job-mode source)."""
    from maven_market.config import bronze_fqn

    def table(name: str) -> str:
        return bronze_fqn(cfg, name)

    customers = [r[0] for r in spark.table(table("customers")).select("customer_id").distinct().collect()]
    # Mongo Bronze is append-only, so a product can have several versions: use the latest price
    products = [tuple(r) for r in spark.sql(
        f"SELECT product_id, product_retail_price FROM (SELECT *, row_number() OVER "
        f"(PARTITION BY product_id ORDER BY _ingested_at DESC) AS _rn FROM {table('products')}) WHERE _rn = 1"
    ).collect()]
    stores = [r[0] for r in spark.table(table("stores")).select("store_id").distinct().collect()]
    return keys_from_rows(customers, products, stores)


def run(cfg: dict, keys: EventKeys, bootstrap: str, api_key: str, api_secret: str,
        max_events: int, events_per_second: float) -> int:  # pragma: no cover - needs a live Kafka cluster
    """Stream simulated events to Kafka; return the number delivered. Raises if any delivery failed."""
    from confluent_kafka import Producer  # lazy: not available in plain unit-test envs

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    })
    delivered, errors = 0, []

    def on_delivery(err, _msg):
        nonlocal delivered
        if err is not None:
            errors.append(str(err))
        else:
            delivered += 1

    delay = 1.0 / max(float(events_per_second), 0.001)
    try:
        for topic, event in iter_events(keys, seed=cfg["producer"]["seed"], max_events=max_events):
            producer.produce(topic, value=serialize(event), on_delivery=on_delivery)
            producer.poll(0)
            time.sleep(delay)
    finally:
        producer.flush(30)
    if errors:
        raise RuntimeError(f"{len(errors)} message(s) failed to deliver; first error: {errors[0]}")
    return delivered


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Kafka
    """Entry point.

    Local mode (env var CONFLUENT_BOOTSTRAP set): keys from the CSVs, creds from env vars.
    Databricks mode (otherwise): keys from Bronze tables, creds from the secret scope.
    """
    from maven_market.config import get, load_config

    args = parse_job_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)
    pcfg = cfg["producer"]
    max_events = args.max_events if args.max_events is not None else pcfg["max_events"]
    eps = args.events_per_second if args.events_per_second is not None else pcfg["events_per_second"]

    if os.environ.get("CONFLUENT_BOOTSTRAP"):
        keys = load_event_keys(pcfg["keys_dir"], pcfg["key_files"])
        bootstrap, key, secret = (os.environ["CONFLUENT_BOOTSTRAP"], os.environ["CONFLUENT_API_KEY"],
                                  os.environ["CONFLUENT_API_SECRET"])
    else:
        from pyspark.sql import SparkSession

        from maven_market.databricks_utils import get_dbutils

        spark = SparkSession.builder.getOrCreate()
        dbutils = get_dbutils(spark)
        keys = load_keys_from_bronze(spark, cfg)
        scope, names = get(cfg, "secret_scope"), get(cfg, "kafka.secrets")
        bootstrap, key, secret = (dbutils.secrets.get(scope, names[k]) for k in ("bootstrap", "api_key", "api_secret"))

    n = run(cfg, keys, bootstrap, key, secret, max_events=max_events, events_per_second=eps)
    print(f"[kafka_producer] delivered {n} events")


if __name__ == "__main__":  # pragma: no cover
    main()
