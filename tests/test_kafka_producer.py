"""Tests for the event simulator (maven_market.producers.kafka_producer)."""
import json
import random
from datetime import datetime, timezone

import pytest

from maven_market.producers.kafka_producer import (
    EventKeys,
    build_inventory_event,
    build_order_event,
    iter_events,
    load_event_keys,
    serialize,
)

NOW = datetime(1998, 6, 1, 12, 0, tzinfo=timezone.utc)


def _keys():
    return EventKeys(
        customer_ids=[1, 2, 3],
        products={10: 2.50, 11: 0.99},
        store_ids=[6, 7],
    )


def _write_dim_csvs(tmp_path):
    (tmp_path / "c.csv").write_text("customer_id,first_name\n1,A\n2,B\n")
    (tmp_path / "p.csv").write_text(
        "product_id,product_name,product_retail_price\n10,X,2.50\n11,Y,0.99\n"
    )
    (tmp_path / "s.csv").write_text("store_id,region_id\n6,28\n7,78\n")
    return {"customers": "c.csv", "products": "p.csv", "stores": "s.csv"}


def test_load_event_keys_reads_real_dimension_ids(tmp_path):
    files = _write_dim_csvs(tmp_path)
    keys = load_event_keys(tmp_path, files)
    assert keys.customer_ids == [1, 2]
    assert keys.products == {10: 2.50, 11: 0.99}
    assert keys.store_ids == [6, 7]


def test_load_event_keys_empty_dimension_raises(tmp_path):
    files = _write_dim_csvs(tmp_path)
    (tmp_path / "s.csv").write_text("store_id,region_id\n")
    with pytest.raises(ValueError):
        load_event_keys(tmp_path, files)


def test_order_event_schema_and_foreign_keys_present():
    keys = _keys()
    ev = build_order_event(keys, random.Random(1), NOW)
    assert set(ev) == {
        "event_type", "order_id", "customer_id", "product_id", "store_id",
        "quantity", "unit_price", "order_ts",
    }
    assert ev["event_type"] == "order"
    assert ev["customer_id"] in keys.customer_ids
    assert ev["product_id"] in keys.products
    assert ev["store_id"] in keys.store_ids
    assert ev["unit_price"] == keys.products[ev["product_id"]]
    assert ev["quantity"] >= 1
    assert ev["order_ts"] == NOW.isoformat()


def test_inventory_event_schema_and_foreign_keys_present():
    keys = _keys()
    ev = build_inventory_event(keys, random.Random(1), NOW)
    assert set(ev) == {"event_type", "product_id", "store_id", "quantity_on_hand", "event_ts"}
    assert ev["event_type"] == "inventory"
    assert ev["product_id"] in keys.products
    assert ev["store_id"] in keys.store_ids
    assert ev["quantity_on_hand"] >= 0


def test_same_seed_gives_same_events():
    keys = _keys()
    a = [build_order_event(keys, random.Random(7), NOW) for _ in range(3)]
    b = [build_order_event(keys, random.Random(7), NOW) for _ in range(3)]
    assert a == b


def test_order_ids_unique_across_a_stream():
    keys = _keys()
    events = list(iter_events(keys, seed=3, max_events=50, now_fn=lambda: NOW))
    order_ids = [e[1]["order_id"] for e in events if e[0] == "orders"]
    assert len(order_ids) == len(set(order_ids)) > 0


def test_iter_events_routes_to_both_topics_and_respects_max():
    keys = _keys()
    events = list(iter_events(keys, seed=3, max_events=40, now_fn=lambda: NOW))
    assert len(events) == 40
    assert {t for t, _ in events} == {"orders", "inventory"}


def test_serialize_round_trips_as_json_bytes():
    ev = build_order_event(_keys(), random.Random(1), NOW)
    assert json.loads(serialize(ev)) == ev
