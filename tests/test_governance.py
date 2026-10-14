"""Tests for the governance helpers: SQL statement splitting/rendering and the gold-tag gate."""
import pytest

from maven_market.config import load_config
from maven_market.governance import (
    GOLD_TABLE_TAGS,
    PII_COLUMNS,
    REGION_COLUMN_TABLES,
    find_untagged_tables,
    governance_placeholders,
    render_sql,
    split_statements,
    tag_assignment_statements,
)


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


# ---- split_statements ----

def test_split_basic_statements():
    assert split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_split_ignores_line_and_block_comments():
    sql = "-- a comment; with a semicolon\nSELECT 1; /* block; comment */ SELECT 2;"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_split_keeps_semicolons_inside_string_literals():
    assert split_statements("SELECT 'a;b'; SELECT 2") == ["SELECT 'a;b'", "SELECT 2"]


def test_split_keeps_semicolons_inside_backticks():
    assert split_statements("SELECT `weird;name` FROM t; SELECT 2") == ["SELECT `weird;name` FROM t", "SELECT 2"]


def test_split_handles_escaped_quotes():
    assert split_statements("SELECT 'it''s; fine'; SELECT 2") == ["SELECT 'it''s; fine'", "SELECT 2"]


def test_split_drops_empty_statements_and_whitespace():
    assert split_statements("\n ;; \n SELECT 1 ;\n\n") == ["SELECT 1"]


def test_split_preserves_multiline_statements():
    out = split_statements("CREATE POLICY p\nON CATALOG c\nROW FILTER f;")
    assert out == ["CREATE POLICY p\nON CATALOG c\nROW FILTER f"]


def test_split_function_body_with_internal_semicolon_inside_dollar_free_sql():
    # SQL UDF bodies here are single expressions - no inner semicolons - so a plain split is sound
    sql = "CREATE FUNCTION f(x INT) RETURNS INT RETURN x + 1;\nSELECT f(1);"
    assert len(split_statements(sql)) == 2


# ---- render_sql ----

def test_placeholders_come_from_config(cfg):
    p = governance_placeholders(cfg)
    assert p["catalog"] == "maven_market_uc"
    assert (p["bronze"], p["silver"], p["gold"], p["audit"]) == ("bronze", "silver", "gold", "audit")


def test_render_replaces_placeholders(cfg):
    rendered = render_sql("SELECT * FROM {catalog}.{gold}.fact_sales", cfg)
    assert rendered == "SELECT * FROM maven_market_uc.gold.fact_sales"


def test_render_unknown_placeholder_raises(cfg):
    with pytest.raises(KeyError):
        render_sql("SELECT {nope}", cfg)


def test_render_leaves_no_braces_for_variant_json_free_sql(cfg):
    assert "{" not in render_sql("SELECT 1 FROM {catalog}.{audit}.t", cfg)


# ---- tag assignment plan ----

GOLD_TABLES = [
    "agg_customer_segments", "agg_inventory_turns", "agg_orders_realtime", "agg_product_performance",
    "agg_sales_monthly", "agg_store_performance", "dim_customer", "dim_date", "dim_product", "dim_store",
    "fact_inventory", "fact_orders", "fact_returns", "fact_sales",
]
ALLOWED_OWNER = {"data_platform", "analytics", "compliance", "regional_sales"}
ALLOWED_SENSITIVITY = {"public", "internal", "confidential", "restricted"}


def test_every_gold_table_gets_both_required_tags_with_allowed_values():
    assert set(GOLD_TABLE_TAGS) == set(GOLD_TABLES), "every gold table needs an explicit owner and sensitivity"
    for table, (owner, sensitivity) in GOLD_TABLE_TAGS.items():
        assert owner in ALLOWED_OWNER, f"{table}: owner_team {owner!r} is not an allowed value"
        assert sensitivity in ALLOWED_SENSITIVITY, f"{table}: data_sensitivity {sensitivity!r} not allowed"


def test_customer_tables_are_not_marked_public():
    for table in ("dim_customer", "agg_customer_segments"):
        assert GOLD_TABLE_TAGS[table][1] in {"confidential", "restricted"}


def test_pii_columns_cover_bronze_silver_and_gold_customer_tables():
    assert {"bronze.customers", "silver.dim_customer", "gold.dim_customer"} <= set(PII_COLUMNS)
    for table, columns in PII_COLUMNS.items():
        assert {"first_name", "last_name"} <= set(columns), f"{table}: names must be PII"
        assert "birthdate" in columns and "customer_address" in columns


def test_region_column_tables_are_exactly_the_gold_tables_that_have_a_region():
    # fact_* / dim_store / aggregates carry data_region; dim_customer, dim_date, dim_product are reference data
    assert set(REGION_COLUMN_TABLES) == set(GOLD_TABLES) - {"dim_customer", "dim_date", "dim_product"}


def test_tag_statements_are_rendered_from_config_and_cover_the_plan(cfg):
    statements = tag_assignment_statements(cfg)
    text = "\n".join(statements)
    assert all(s.startswith("SET TAG ON ") for s in statements)
    assert "{" not in text and "maven_market_uc.gold.fact_sales.data_region" in text
    assert text.count("`owner_team`") == len(GOLD_TABLES) and text.count("`data_sensitivity`") == len(GOLD_TABLES)
    assert text.count("`region_code`") == len(REGION_COLUMN_TABLES)
    pii_total = sum(len(c) for c in PII_COLUMNS.values())
    assert text.count("`classification` = `PII`") == pii_total


def test_financial_columns_are_tagged_but_not_pii(cfg):
    text = "\n".join(tag_assignment_statements(cfg))
    assert "yearly_income `classification` = `Financial`" in text


def test_no_tag_is_ever_set_on_the_gold_schema_itself(cfg):
    # schema tags inherit to every future table and would defeat the structural rule
    for statement in tag_assignment_statements(cfg):
        assert not statement.startswith("SET TAG ON SCHEMA") and "ON SCHEMA" not in statement


# ---- the structural gate: no gold table without owner_team + data_sensitivity ----

REQUIRED = ["owner_team", "data_sensitivity"]


def test_find_untagged_returns_empty_when_all_tables_are_fully_tagged():
    tables = ["fact_sales", "dim_store"]
    tags = {("fact_sales", "owner_team"), ("fact_sales", "data_sensitivity"),
            ("dim_store", "owner_team"), ("dim_store", "data_sensitivity")}
    assert find_untagged_tables(tables, tags, REQUIRED) == {}


def test_find_untagged_reports_which_tags_each_table_is_missing():
    tables = ["fact_sales", "dim_store", "brand_new"]
    tags = {("fact_sales", "owner_team"), ("fact_sales", "data_sensitivity"), ("dim_store", "owner_team")}
    assert find_untagged_tables(tables, tags, REQUIRED) == {
        "dim_store": ["data_sensitivity"],
        "brand_new": ["owner_team", "data_sensitivity"],
    }


def test_find_untagged_ignores_tags_on_tables_not_in_the_list():
    assert find_untagged_tables(["a"], {("zzz", "owner_team"), ("zzz", "data_sensitivity")}, REQUIRED) == {
        "a": ["owner_team", "data_sensitivity"]
    }


def test_find_untagged_with_no_tables_is_clean():
    assert find_untagged_tables([], set(), REQUIRED) == {}
