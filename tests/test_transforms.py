"""Contract tests for the Silver/Gold SQL (pure Python): rendering, placeholders, config coverage."""
import re

import pytest

from maven_market.config import load_config
from maven_market.pipelines import transforms as T


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def test_every_silver_and_gold_table_in_config_has_a_definition(cfg):
    # silver: SCD2 dims come from stream exprs, Kafka datasets are stream-static joins, rest is SQL
    groups = [set(T.SILVER_SQL), set(T.SCD2_SOURCES), set(T.SILVER_STREAMS)]
    assert set().union(*groups) == set(cfg["pipeline"]["silver"])
    assert sum(len(g) for g in groups) == len(set().union(*groups)), "a silver table has two definitions"
    assert set(T.GOLD_SQL) == set(cfg["pipeline"]["gold"])


@pytest.mark.parametrize("name", sorted(set(T.SILVER_SQL)))
def test_silver_sql_renders_fully(cfg, name):
    sql = T.render(T.SILVER_SQL[name], cfg)
    assert not re.search(r"[{}]", sql), f"unrendered placeholder in silver.{name}"
    assert "maven_market_uc.bronze." in sql


@pytest.mark.parametrize("name", sorted(set(T.GOLD_SQL)))
def test_gold_sql_renders_fully(cfg, name):
    sql = T.render(T.GOLD_SQL[name], cfg)
    assert not re.search(r"[{}]", sql), f"unrendered placeholder in gold.{name}"


# ---- stream-static joins (PDF: "Silver: join Streams with Static data") ----

def test_kafka_datasets_are_streams_joined_to_static_dimensions():
    assert set(T.SILVER_STREAMS) == {"orders", "inventory"}
    for name, spec in T.SILVER_STREAMS.items():
        assert spec["static"], f"silver.{name} must join at least one static dimension"
        assert "orders" not in T.SILVER_SQL and "inventory" not in T.SILVER_SQL


def test_stream_join_keys_are_produced_by_the_stream_expressions():
    for name, spec in T.SILVER_STREAMS.items():
        produced = set(T.output_columns(spec["exprs"]))
        for static in spec["static"]:
            assert set(static["on"]) <= produced, f"silver.{name}: join key not in stream output"
        assert set(spec.get("dedupe_on", [])) <= produced


@pytest.mark.parametrize("name", sorted(set(T.SILVER_STREAMS)))
def test_static_sides_render_fully_and_read_only_silver(cfg, name):
    for static in T.SILVER_STREAMS[name]["static"]:
        sql = T.render(static["sql"], cfg)
        assert not re.search(r"[{}]", sql)
        assert "bronze" not in sql and "maven_market_uc.silver." in sql


def test_static_side_over_an_scd2_dimension_only_reads_current_rows(cfg):
    """Joining all SCD2 versions would multiply every streamed row by its number of versions."""
    scd2_tables = [T.names(cfg)[f"silver_{d}"] for d in T.SCD2_SOURCES]
    for name, spec in T.SILVER_STREAMS.items():
        for static in spec["static"]:
            sql = T.render(static["sql"], cfg)
            if any(t in sql for t in scd2_tables):
                assert "__END_AT IS NULL" in sql, f"silver.{name}: static side must filter to current SCD2 rows"


@pytest.mark.parametrize("name", sorted(set(T.SILVER_STREAMS)))
def test_batch_equivalent_sql_for_dry_runs(cfg, name):
    sql = T.stream_equivalent_sql(cfg, name)
    assert not re.search(r"[{}]", sql)
    assert sql.count("LEFT JOIN") == len(T.SILVER_STREAMS[name]["static"])
    assert T.names(cfg)[f"bronze_{T.SILVER_STREAMS[name]['bronze']}"] in sql


def test_orders_are_enriched_with_product_store_and_customer_attributes(cfg):
    text = " ".join(T.render(s["sql"], cfg) for s in T.SILVER_STREAMS["orders"]["static"])
    for column in ("product_name", "product_brand", "store_name", "member_card"):
        assert column in text


# ---- region-governed gold tables (row-level security needs a region column to filter on) ----

REGIONAL_GOLD = [
    "fact_sales", "fact_returns", "fact_orders", "fact_inventory", "dim_store", "agg_sales_monthly",
    "agg_store_performance", "agg_orders_realtime", "agg_product_performance", "agg_customer_segments",
    "agg_inventory_turns",
]
REFERENCE_GOLD = ["dim_date", "dim_customer", "dim_product"]   # not regional: shared reference data


def test_every_gold_table_is_classified_as_regional_or_reference(cfg):
    assert set(REGIONAL_GOLD) | set(REFERENCE_GOLD) == set(cfg["pipeline"]["gold"])
    assert not set(REGIONAL_GOLD) & set(REFERENCE_GOLD)


@pytest.mark.parametrize("name", REGIONAL_GOLD)
def test_regional_gold_tables_output_a_data_region_column(cfg, name):
    """No regional table may drop the region column, or the row filter has nothing to match."""
    sql = T.render(T.GOLD_SQL[name], cfg)
    assert "data_region" in sql, f"gold.{name} must carry data_region"


def _group_by_items(sql: str) -> list[str]:
    """Items of the OUTERMOST final GROUP BY (positions or names)."""
    tail = sql.rsplit("GROUP BY", 1)[1]
    return [part.strip() for part in tail.split(")")[0].split(",")]


def _select_items(sql: str) -> list[str]:
    head = sql.split(" FROM ", 1)[0].replace("SELECT ", "", 1)
    out, depth, cur = [], 0, ""
    for ch in head:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    return out + [cur.strip()]


@pytest.mark.parametrize("name", ["agg_customer_segments", "agg_orders_realtime"])
def test_single_level_aggregates_group_by_the_region_column(cfg, name):
    sql = T.render(T.GOLD_SQL[name], cfg)
    items = _select_items(sql)
    position = next(i for i, item in enumerate(items, 1) if item.split(".")[-1].split(" AS ")[-1] == "data_region")
    grouped = _group_by_items(sql)
    assert str(position) in grouped or "data_region" in " ".join(grouped), f"gold.{name}: region not in GROUP BY"


@pytest.mark.parametrize("name", ["agg_product_performance", "agg_inventory_turns"])
def test_product_aggregates_are_split_per_region_in_every_subquery(cfg, name):
    """Per-product totals must be computed per region, in the outer select and in each joined subquery."""
    sql = T.render(T.GOLD_SQL[name], cfg)
    assert "GROUP BY product_id, data_region" in sql
    assert sql.count("GROUP BY product_id, data_region") == sql.count("GROUP BY")   # none left global
    assert "s.data_region AS data_region" in sql or "s.data_region," in sql
    if "LEFT JOIN (SELECT" in sql:
        assert "s.data_region = " in sql, f"gold.{name}: joined subquery not matched on region"


def test_no_regional_aggregate_has_a_global_only_group_by(cfg):
    for name in ("agg_product_performance", "agg_inventory_turns"):
        sql = T.render(T.GOLD_SQL[name], cfg)
        assert "GROUP BY product_id)" not in sql and "GROUP BY product_id " not in sql


def test_gold_reads_only_silver_or_gold_never_bronze(cfg):
    for name, template in T.GOLD_SQL.items():
        assert "bronze" not in T.render(template, cfg), f"gold.{name} must not read bronze directly"


def test_render_unknown_placeholder_raises(cfg):
    with pytest.raises(KeyError):
        T.render("SELECT * FROM {no_such_table}", cfg)


def test_bronze_names_are_resolved_from_config(cfg):
    n = T.names(cfg)
    assert n["bronze_customers"] == "maven_market_uc.bronze.customers"
    assert n["bronze_orders"] == "maven_market_uc.bronze.orders"
    assert n["silver_dim_customer"] == "maven_market_uc.silver.dim_customer"
    assert n["gold_fact_sales"] == "maven_market_uc.gold.fact_sales"


def test_data_region_expr_is_built_from_config_and_null_safe(cfg):
    expr = T.data_region_expr(cfg, "s.store_country")
    assert "try_element_at" in expr  # ANSI mode errors on a missing map key otherwise
    assert "'Canada'" in expr and "'NA'" in expr and "'UNKNOWN'" in expr


SYNTHETIC_COUNTRIES = {
    "EU": {"Germany", "France", "Spain", "Italy", "Netherlands", "Sweden"},
    "APAC": {"Japan", "South Korea", "Australia", "Singapore", "India"},
}


def test_data_region_values_are_legal_governed_tag_values(cfg):
    # the governed tag `data_region` only allows these values (sql/governance/governed_tags.sql)
    assert set(cfg["pipeline"]["data_region_by_country"].values()) <= {"EU", "NA", "APAC"}


def test_every_store_country_in_the_source_and_synthetic_files_is_mapped(cfg):
    """An unmapped country silently becomes data_region='UNKNOWN', which the row filter would hide."""
    mapping = cfg["pipeline"]["data_region_by_country"]
    for country in ("USA", "Canada", "Mexico"):
        assert mapping[country] == "NA"
    for region, countries in SYNTHETIC_COUNTRIES.items():
        for country in countries:
            assert mapping.get(country) == region, f"{country} must map to {region}"


def test_data_region_expr_maps_every_configured_country_and_defaults_to_unknown(cfg):
    expr = T.data_region_expr(cfg, "s.store_country")
    for country, region in cfg["pipeline"]["data_region_by_country"].items():
        assert f"'{country}', '{region}'" in expr
    assert expr.endswith("'UNKNOWN')")


def test_data_region_expr_rejects_quotes_in_config(cfg):
    bad = {**cfg, "pipeline": {**cfg["pipeline"], "data_region_by_country": {"U'SA": "NA"}}}
    with pytest.raises(ValueError):
        T.data_region_expr(bad, "x")


def test_scd2_sources_have_keys_and_sequence_column_in_exprs():
    for name, spec in T.SCD2_SOURCES.items():
        # master data comes from MongoDB (no source file): changes are ordered by Bronze ingest time
        assert spec["keys"] and spec["sequence_by"] == "_ingested_at"
        joined = " ".join(spec["exprs"])
        for key in spec["keys"]:
            assert key in joined
        assert spec["sequence_by"] in joined


def test_output_columns_handles_aliases_and_bare_columns():
    exprs = ["customer_id", "trim(first_name) AS first_name", "(x = 'Y') as is_homeowner", "_ingested_at"]
    assert T.output_columns(exprs) == ["customer_id", "first_name", "is_homeowner", "_ingested_at"]


def test_scd2_history_ignore_list_only_names_real_output_columns(cfg):
    for name, spec in T.SCD2_SOURCES.items():
        ignore = T.scd2_ignore_columns(cfg, name)
        assert set(ignore) <= set(T.output_columns(spec["exprs"]))
        assert spec["sequence_by"] in ignore        # the sequence column must never create versions
        assert "_rescued_data" not in ignore        # not selected, so must not be listed
        assert not set(ignore) & set(spec["keys"])


def test_scd2_ignore_columns_include_lineage_and_sequence(cfg):
    ignore = set(cfg["pipeline"]["scd2_ignore_columns"])
    assert {"_source_file_modified_at", "_ingested_at", "_rescued_data"} <= ignore


def test_scd2_dims_read_the_mongo_owned_unsuffixed_bronze_tables(cfg):
    n = T.names(cfg)
    assert n["bronze_customers"] == "maven_market_uc.bronze.customers"
    assert n["bronze_products"] == "maven_market_uc.bronze.products"
    assert T.SCD2_SOURCES["dim_customer"]["bronze"] == "customers"
    assert T.SCD2_SOURCES["dim_product"]["bronze"] == "products"


def test_expectation_columns_exist_in_silver_outputs():
    """Rules for the SCD2 dims reference columns the view must actually produce."""
    from maven_market.config import load_expectations

    exp = load_expectations()
    cust = " ".join(T.SCD2_SOURCES["dim_customer"]["exprs"])
    for needed in ("birthdate", "income_low_k", "gender"):
        assert needed in cust
    assert "customer_id" in cust
    prod = " ".join(T.SCD2_SOURCES["dim_product"]["exprs"])
    for needed in ("product_retail_price", "product_cost"):
        assert needed in prod
    assert exp["dim_customer"]["drop"]
