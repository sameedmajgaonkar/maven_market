"""Pure-Python tests for the Kafka producer job entry point (the Spark parts run on Databricks)."""
import pytest

from maven_market.producers.kafka_producer import keys_from_rows
from maven_market.producers.kafka_producer import parse_job_args as producer_args


# ---- producer job ----

def test_producer_args_defaults_defer_to_config():
    a = producer_args([])
    assert a.env == "dev" and a.max_events is None and a.events_per_second is None


def test_producer_args_explicit():
    a = producer_args(["--max-events", "500", "--events-per-second", "20", "--config-dir", "/c"])
    assert (a.max_events, a.events_per_second, a.config_dir) == (500, 20.0, "/c")


def test_keys_from_rows_builds_event_keys():
    k = keys_from_rows([1, 2], [(10, 2.5), (11, 0.99)], [6])
    assert k.customer_ids == [1, 2] and k.products == {10: 2.5, 11: 0.99} and k.store_ids == [6]


def test_keys_from_rows_skips_products_without_price():
    k = keys_from_rows([1], [(10, 2.5), (11, None)], [6])
    assert k.products == {10: 2.5}


@pytest.mark.parametrize("args", [
    ([], [(1, 1.0)], [1]),
    ([1], [], [1]),
    ([1], [(1, 1.0)], []),
    ([1], [(1, None)], [1]),
])
def test_keys_from_rows_empty_dimension_raises(args):
    with pytest.raises(ValueError):
        keys_from_rows(*args)
