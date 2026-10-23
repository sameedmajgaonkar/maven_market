"""Pure-Python tests for the MongoDB seed + ingest helpers (no Spark/Mongo needed)."""
from datetime import date, datetime

import pytest

from maven_market.config import bronze_fqn, load_config
from maven_market.ingestion.mongo_ingest import (
    documents_to_rows,
    plan_snapshot,
    schema_columns,
    schema_types,
    snapshot_digest,
)
from maven_market.producers.mongo_seed import build_documents


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def _schema(cfg, name):
    return cfg["mongo"]["collections"][name]["schema"]


# ---- schema helpers ----

def test_schema_columns_and_types_parse_ddl(cfg):
    ddl = _schema(cfg, "customers")
    assert schema_columns(ddl)[0] == "_id"
    types = schema_types(ddl)
    assert types["birthdate"] == "DATE" and types["customer_id"] == "INT" and types["yearly_income"] == "STRING"


def test_every_collection_has_schema_table_id_field_and_a_csv_source(cfg):
    for name, spec in cfg["mongo"]["collections"].items():
        assert spec["bronze_table"].startswith("bronze.")
        assert spec["id_field"] in schema_columns(spec["schema"]), name
        assert name in cfg["producer"]["key_files"], f"no source CSV configured for '{name}'"


def test_both_master_collections_are_configured(cfg):
    assert set(cfg["mongo"]["collections"]) == {"products", "customers"}


def test_every_bronze_table_has_exactly_one_writer(cfg):
    """Mongo owns bronze.customers / bronze.products (no suffix), so the CSV list must not also write
    them: two writers on one table corrupt each other's checkpoints/streams."""
    writers = (
        [("csv", s["bronze_table"]) for s in cfg["sources"].values()]
        + [("mongo", s["bronze_table"]) for s in cfg["mongo"]["collections"].values()]
        + [("kafka", t["bronze_table"]) for t in cfg["kafka"]["topics"].values()]
        + [("kafka-dead-letter", t["dead_letter_table"]) for t in cfg["kafka"]["topics"].values()]
    )
    tables = [table for _, table in writers]
    duplicated = {t for t in tables if tables.count(t) > 1}
    assert not duplicated, f"tables with more than one writer: {duplicated}"


def test_mongo_owns_the_unsuffixed_master_tables(cfg):
    assert cfg["mongo"]["collections"]["customers"]["bronze_table"] == "bronze.customers"
    assert cfg["mongo"]["collections"]["products"]["bronze_table"] == "bronze.products"
    assert "customers" not in cfg["sources"] and "products" not in cfg["sources"]


# ---- seed documents: CSV row -> typed Mongo document, driven by the schema ----

def test_build_documents_products(cfg):
    rows = [{
        "product_id": "1", "product_brand": "Washington", "product_name": "Berry Juice",
        "product_sku": "90748583674", "product_retail_price": "2.85", "product_cost": "0.94",
        "product_weight": "8.39", "recyclable": "", "low_fat": "1",
    }]
    doc = build_documents(rows, _schema(cfg, "products"), "product_id")[0]
    assert doc["product_id"] == 1 and isinstance(doc["product_id"], int)
    assert doc["product_sku"] == 90748583674 and doc["product_retail_price"] == 2.85
    assert doc["recyclable"] is None and doc["low_fat"] == 1
    assert "_id" not in doc  # Mongo assigns it


def test_build_documents_customers_parses_us_dates_and_types(cfg):
    rows = [{
        "customer_id": "1", "customer_acct_num": "87462024688", "first_name": " Sheri ", "last_name": "Nowmer",
        "customer_address": "2433 Bailey Road", "customer_city": "Tlaxiaco", "customer_state_province": "Oaxaca",
        "customer_postal_code": "15057", "customer_country": "Mexico", "birthdate": "8/26/1961",
        "marital_status": "M", "yearly_income": "$30K - $50K", "gender": "F", "total_children": "4",
        "num_children_at_home": "2", "education": "Partial High School", "acct_open_date": "9/10/1991",
        "member_card": "Bronze", "occupation": "Skilled Manual", "homeowner": "Y",
    }]
    doc = build_documents(rows, _schema(cfg, "customers"), "customer_id")[0]
    assert doc["birthdate"] == datetime(1961, 8, 26) and doc["acct_open_date"] == datetime(1991, 9, 10)
    assert doc["customer_acct_num"] == 87462024688 and doc["total_children"] == 4
    assert doc["first_name"] == "Sheri"           # trimmed
    assert doc["yearly_income"] == "$30K - $50K"  # '$' preserved


def test_build_documents_skips_rows_without_id_and_blanks_become_none(cfg):
    rows = [{"customer_id": "", "first_name": "x"}, {"customer_id": "2", "birthdate": ""}]
    docs = build_documents(rows, _schema(cfg, "customers"), "customer_id")
    assert [d["customer_id"] for d in docs] == [2]
    assert docs[0]["birthdate"] is None and docs[0]["first_name"] is None


def test_build_documents_rejects_unsupported_type():
    with pytest.raises(ValueError):
        build_documents([{"id": "1", "x": "1"}], "id INT, x BINARY", "id")


# ---- ingest: documents -> rows ----

def test_documents_to_rows_stringifies_id_and_fills_missing(cfg):
    cols = schema_columns(_schema(cfg, "products"))
    rows = documents_to_rows([{"_id": object(), "product_id": 5}], cols)
    assert isinstance(rows[0]["_id"], str) and rows[0]["product_id"] == 5
    assert rows[0]["product_cost"] is None            # absent field -> NULL, not KeyError
    assert set(rows[0]) == set(cols)                   # unknown extra fields are not leaked in


def test_documents_to_rows_converts_mongo_datetimes_to_dates_for_date_columns(cfg):
    ddl = _schema(cfg, "customers")
    rows = documents_to_rows(
        [{"_id": 1, "customer_id": 1, "birthdate": datetime(1961, 8, 26), "acct_open_date": None}],
        schema_columns(ddl), schema_types(ddl),
    )
    assert rows[0]["birthdate"] == date(1961, 8, 26) and type(rows[0]["birthdate"]) is date
    assert rows[0]["acct_open_date"] is None


def test_documents_to_rows_extra_unknown_field_dropped(cfg):
    ddl = _schema(cfg, "products")
    rows = documents_to_rows([{"_id": 1, "product_id": 1, "surprise": "x"}], schema_columns(ddl), schema_types(ddl))
    assert "surprise" not in rows[0]


# ---- type coercion of BSON values ----

def test_documents_to_rows_normalizes_decimal128_and_objectid():
    class FakeDecimal128:
        def to_decimal(self):
            from decimal import Decimal
            return Decimal("19.99")

    cols = ["_id", "product_id", "product_retail_price"]
    types = {"_id": "STRING", "product_id": "INT", "product_retail_price": "DOUBLE"}
    rows = documents_to_rows([{"_id": "obj123", "product_id": "42", "product_retail_price": FakeDecimal128()}],
                             cols, types)
    assert rows[0] == {"_id": "obj123", "product_id": 42, "product_retail_price": 19.99}


def test_documents_to_rows_stringifies_nested_documents_deterministically():
    import json
    rows = documents_to_rows([{"product_id": 1, "details": {"sizes": ["M", "L"], "color": "red"}}],
                             ["product_id", "details"], {"product_id": "INT", "details": "STRING"})
    assert json.loads(rows[0]["details"]) == {"color": "red", "sizes": ["M", "L"]}
    assert rows[0]["details"].index("color") < rows[0]["details"].index("sizes")  # sort_keys


def test_documents_to_rows_failed_casts_become_null_not_errors():
    rows = documents_to_rows(
        [{"is_active": "true", "count": "100"}, {"is_active": "false", "count": "invalid_int"}],
        ["is_active", "count"], {"is_active": "BOOLEAN", "count": "INT"},
    )
    assert rows[0] == {"is_active": True, "count": 100}
    assert rows[1] == {"is_active": False, "count": None}


# ---- snapshot-on-change: the stream offset is a digest of the collection ----

COLS = ["_id", "customer_id", "member_card", "yearly_income"]
V1 = {"_id": "a", "customer_id": 1, "member_card": "Bronze", "yearly_income": "$30K - $50K"}
V2 = {"_id": "b", "customer_id": 2, "member_card": "Silver", "yearly_income": "$50K - $70K"}


def test_first_batch_emits_the_whole_snapshot():
    emit, digest = plan_snapshot([V1, V2], COLS, "customer_id", "")
    assert emit == [V1, V2] and digest


def test_unchanged_collection_emits_nothing_and_keeps_the_offset():
    _, committed = plan_snapshot([V1, V2], COLS, "customer_id", "")
    emit, digest = plan_snapshot([V1, V2], COLS, "customer_id", committed)
    assert emit == [] and digest == committed


def test_digest_ignores_document_order():
    assert snapshot_digest([V1, V2], COLS, "customer_id") == snapshot_digest([V2, V1], COLS, "customer_id")


def test_a_changed_field_emits_a_fresh_snapshot_with_a_new_offset():
    _, committed = plan_snapshot([V1, V2], COLS, "customer_id", "")
    edited = {**V1, "member_card": "Golden"}
    emit, digest = plan_snapshot([edited, V2], COLS, "customer_id", committed)
    assert emit == [edited, V2] and digest != committed


def test_a_new_document_emits_a_fresh_snapshot():
    _, committed = plan_snapshot([V1], COLS, "customer_id", "")
    emit, digest = plan_snapshot([V1, V2], COLS, "customer_id", committed)
    assert len(emit) == 2 and digest != committed


def test_null_vs_value_is_a_change():
    _, committed = plan_snapshot([V1], COLS, "customer_id", "")
    emit, _ = plan_snapshot([{**V1, "member_card": None}], COLS, "customer_id", committed)
    assert emit


def test_an_empty_collection_is_refused():
    with pytest.raises(ValueError):
        plan_snapshot([], COLS, "customer_id", "")


def test_reverting_an_edit_is_seen_as_a_change_against_the_committed_offset():
    _, v1_digest = plan_snapshot([V1], COLS, "customer_id", "")
    _, v2_digest = plan_snapshot([{**V1, "member_card": "Golden"}], COLS, "customer_id", v1_digest)
    emit, digest = plan_snapshot([V1], COLS, "customer_id", v2_digest)
    assert emit == [V1] and digest == v1_digest


def test_bronze_fqn_resolves_across_csv_mongo_and_kafka(cfg):
    assert bronze_fqn(cfg, "stores") == "maven_market_uc.bronze.stores"           # csv
    assert bronze_fqn(cfg, "customers") == "maven_market_uc.bronze.customers"     # mongo
    assert bronze_fqn(cfg, "orders") == "maven_market_uc.bronze.orders"           # kafka
    with pytest.raises(KeyError):
        bronze_fqn(cfg, "nope")


def test_bronze_fqn_rejects_a_name_defined_by_two_sources(cfg):
    clash = {**cfg, "sources": {**cfg["sources"], "customers": {"bronze_table": "bronze.customers"}}}
    with pytest.raises(ValueError):
        bronze_fqn(clash, "customers")
