"""Silver and Gold transformation logic, expressed as plain SQL text.

Keeping the logic as SQL strings (rather than DataFrame code) means it can be rendered and
contract-tested without Spark, and dry-run against Bronze through a SQL warehouse before it is
wired into the pipeline. Table names are placeholders such as ``{bronze_customers}`` /
``{silver_dim_store}`` / ``{gold_fact_sales}`` resolved from config by ``render``.

Silver:
  * SCD2 dimensions (customer, product): ``SCD2_SOURCES`` = select-expressions applied to a
    streaming read of Bronze, then fed to ``dp.create_auto_cdc_flow``.
  * Kafka events (orders, inventory): ``SILVER_STREAMS`` - streaming tables that stream-static join
    the events to the current dimensions.
  * Everything else: ``SILVER_SQL`` (latest-version-per-key dedupe, typing, trimming).
Gold: ``GOLD_SQL`` - conformed dimensions, facts and BI aggregates (read Silver/Gold only).
"""
from __future__ import annotations

from maven_market.config import bronze_fqn, get, table_fqn


# ----------------------------------------------------------------------------- name resolution

def names(cfg: dict) -> dict:
    """Placeholder -> fully qualified table name, for every bronze/silver/gold dataset."""
    out = {}
    for group in ("sources", "mongo.collections", "kafka.topics"):
        for dataset in get(cfg, group):
            out[f"bronze_{dataset}"] = bronze_fqn(cfg, dataset)
    for layer in ("silver", "gold"):
        for key in get(cfg, f"pipeline.{layer}"):
            out[f"{layer}_{key}"] = table_fqn(cfg, layer, key)
    out["data_region_s"] = data_region_expr(cfg, "s.store_country")
    return out


def data_region_expr(cfg: dict, country_column: str) -> str:
    """SQL expression mapping a country column to its governed ``data_region`` (from config)."""
    mapping = get(cfg, "pipeline.data_region_by_country")
    for k, v in mapping.items():
        if "'" in str(k) or "'" in str(v):
            raise ValueError(f"Quotes are not allowed in data_region_by_country: {k!r}: {v!r}")
    pairs = ", ".join(f"'{k}', '{v}'" for k, v in mapping.items())
    # try_element_at: ANSI mode raises on a missing map key, this returns NULL instead
    return f"coalesce(try_element_at(map({pairs}), {country_column}), 'UNKNOWN')"


def render(template: str, cfg: dict) -> str:
    return template.format(**names(cfg))


# ------------------------------------------------------------------------------- Silver: SCD2

# Applied with DataFrame.selectExpr to a streaming read of the Bronze table. NB: no backslashes in
# regexes (Spark SQL string-literal escaping differs from Python) - hence the [$] character class.
# customers/products come from MongoDB, so there is no source file: ordering of changes is by the
# time Bronze ingested each document version.
_LINEAGE = ["_ingested_at"]

SCD2_SOURCES = {
    "dim_customer": {
        "bronze": "customers",
        "keys": ["customer_id"],
        "sequence_by": "_ingested_at",
        "exprs": [
            "customer_id",
            "customer_acct_num",
            "trim(first_name) AS first_name",
            "trim(last_name) AS last_name",
            "concat_ws(' ', trim(first_name), trim(last_name)) AS full_name",
            "trim(customer_address) AS customer_address",
            "trim(customer_city) AS customer_city",
            "trim(customer_state_province) AS customer_state_province",
            "lpad(cast(customer_postal_code AS STRING), 5, '0') AS customer_postal_code",
            "trim(customer_country) AS customer_country",
            "birthdate",
            "upper(trim(marital_status)) AS marital_status",
            "trim(yearly_income) AS yearly_income",
            "try_cast(regexp_extract(yearly_income, '[$]([0-9]+)K', 1) AS INT) AS income_low_k",
            "try_cast(nullif(regexp_extract(yearly_income, '- [$]([0-9]+)K', 1), '') AS INT) AS income_high_k",
            "upper(trim(gender)) AS gender",
            "total_children",
            "num_children_at_home",
            "trim(education) AS education",
            "acct_open_date",
            "trim(member_card) AS member_card",
            "trim(occupation) AS occupation",
            "(upper(trim(homeowner)) = 'Y') AS is_homeowner",
            *_LINEAGE,
        ],
    },
    "dim_product": {
        "bronze": "products",
        "keys": ["product_id"],
        "sequence_by": "_ingested_at",
        "exprs": [
            "product_id",
            "trim(product_brand) AS product_brand",
            "trim(product_name) AS product_name",
            "cast(product_sku AS STRING) AS product_sku",
            "product_retail_price",
            "product_cost",
            "product_weight",
            "cast(recyclable AS BOOLEAN) AS is_recyclable",
            "cast(low_fat AS BOOLEAN) AS is_low_fat",
            *_LINEAGE,
        ],
    },
}


def output_columns(exprs: list[str]) -> list[str]:
    """Column names produced by a list of select-expressions (``expr AS name`` or a bare column)."""
    cols = []
    for expr in exprs:
        lowered = expr.lower()
        idx = lowered.rfind(" as ")
        cols.append(expr[idx + 4:].strip() if idx != -1 else expr.strip())
    return cols


def scd2_ignore_columns(cfg: dict, dim: str) -> list[str]:
    """Configured housekeeping columns that this dim actually outputs (never trigger a new version)."""
    produced = set(output_columns(SCD2_SOURCES[dim]["exprs"]))
    return [c for c in get(cfg, "pipeline.scd2_ignore_columns") if c in produced]


# ----------------------------------------------------------------------------- Silver: batch

def _latest_per_key(select_list: str, bronze_placeholder: str, key: str, order_by: str) -> str:
    """Latest row per ``key`` (so a re-delivered/corrected file replaces, not duplicates)."""
    return (
        f"SELECT {select_list} FROM (\n"
        f"  SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY {order_by}) AS _rn\n"
        "  FROM " + bronze_placeholder + "\n"
        ") WHERE _rn = 1"
    )


_FILE_ORDER = "_source_file_modified_at DESC, _ingested_at DESC"

SILVER_SQL = {
    "dim_store": _latest_per_key(
        "store_id, region_id, trim(store_type) AS store_type, trim(store_name) AS store_name, "
        "trim(store_street_address) AS store_street_address, trim(store_city) AS store_city, "
        "trim(store_state) AS store_state, trim(store_country) AS store_country, "
        "trim(store_phone) AS store_phone, first_opened_date, last_remodel_date, total_sqft, grocery_sqft, "
        "_source_file_modified_at, _ingested_at",
        "{bronze_stores}", "store_id", _FILE_ORDER,
    ),
    "dim_region": _latest_per_key(
        "region_id, trim(sales_district) AS sales_district, trim(sales_region) AS sales_region, "
        "_source_file_modified_at, _ingested_at",
        "{bronze_regions}", "region_id", _FILE_ORDER,
    ),
    "dim_date": "SELECT DISTINCT date FROM {bronze_calendar}",
    "transactions": (
        "SELECT transaction_date, stock_date, product_id, customer_id, store_id, quantity, "
        "_source_file_name, _ingested_at FROM {bronze_transactions}"
    ),
    "returns": (
        "SELECT return_date, product_id, store_id, quantity, _source_file_name, _ingested_at "
        "FROM {bronze_returns}"
    ),
}


# --------------------------------------------------------------- Silver: stream-static joins

# Kafka events (the STREAM) are enriched with dimension attributes (the STATIC side): a left join,
# so an event for an unknown key is kept (with NULL attributes) rather than silently lost.
#
# `static[*].sql` selects from a Silver dimension. SCD2 dimensions are filtered to their CURRENT rows
# (`__END_AT IS NULL`); joining every version would multiply each event by the dimension's history.
# `dedupe_on` removes re-delivered events (stateful; fine at this scale).
_STATIC_PRODUCT = {
    "sql": (
        "SELECT product_id, product_brand, product_name, product_cost AS unit_cost "
        "FROM {silver_dim_product} WHERE __END_AT IS NULL"
    ),
    "on": ["product_id"],
}
_STATIC_STORE = {
    "sql": "SELECT store_id, store_name, store_country FROM {silver_dim_store}",
    "on": ["store_id"],
}
_STATIC_CUSTOMER = {
    "sql": "SELECT customer_id, member_card FROM {silver_dim_customer} WHERE __END_AT IS NULL",
    "on": ["customer_id"],
}

SILVER_STREAMS = {
    "orders": {
        "bronze": "orders",
        "exprs": [
            "order_id", "customer_id", "product_id", "store_id", "quantity", "unit_price",
            "cast(order_ts AS TIMESTAMP) AS order_ts", "_kafka_partition", "_kafka_offset", "_ingested_at",
        ],
        "dedupe_on": ["order_id"],
        "static": [_STATIC_PRODUCT, _STATIC_STORE, _STATIC_CUSTOMER],
    },
    "inventory": {
        "bronze": "inventory",
        "exprs": [
            "product_id", "store_id", "quantity_on_hand", "cast(event_ts AS TIMESTAMP) AS event_ts",
            "_kafka_partition", "_kafka_offset", "_ingested_at",
        ],
        "static": [_STATIC_PRODUCT, _STATIC_STORE],
    },
}


def stream_equivalent_sql(cfg: dict, name: str) -> str:
    """Batch SQL equivalent of a stream-static join, for dry-runs on a SQL warehouse."""
    spec = SILVER_STREAMS[name]
    sql = f"SELECT {', '.join(spec['exprs'])} FROM {names(cfg)['bronze_' + spec['bronze']]}"
    for i, static in enumerate(spec["static"], 1):
        using = ", ".join(static["on"])
        sql = f"SELECT * FROM ({sql}) LEFT JOIN ({render(static['sql'], cfg)}) j{i} USING ({using})"
    return sql


# ------------------------------------------------------------------------------------- Gold

_CUSTOMER_COLS = (
    "customer_id, customer_acct_num, first_name, last_name, full_name, customer_address, customer_city, "
    "customer_state_province, customer_postal_code, customer_country, birthdate, marital_status, "
    "yearly_income, income_low_k, income_high_k, gender, total_children, num_children_at_home, "
    "education, acct_open_date, member_card, occupation, is_homeowner"
)
_PRODUCT_COLS = (
    "product_id, product_brand, product_name, product_sku, product_retail_price, product_cost, "
    "product_weight, is_recyclable, is_low_fat"
)

GOLD_SQL = {
    "dim_date": (
        "SELECT date AS full_date, year(date) AS year, quarter(date) AS quarter, month(date) AS month, "
        "date_format(date, 'MMMM') AS month_name, weekofyear(date) AS week_of_year, "
        "dayofweek(date) AS day_of_week, date_format(date, 'EEEE') AS day_name, "
        "dayofweek(date) IN (1, 7) AS is_weekend, year(date) * 100 + month(date) AS year_month "
        "FROM {silver_dim_date}"
    ),
    # current version of each SCD2 row (open-ended validity)
    "dim_customer": f"SELECT {_CUSTOMER_COLS} FROM {{silver_dim_customer}} WHERE __END_AT IS NULL",
    "dim_product": f"SELECT {_PRODUCT_COLS} FROM {{silver_dim_product}} WHERE __END_AT IS NULL",
    "dim_store": (
        "SELECT s.store_id, s.region_id, r.sales_district, r.sales_region, s.store_type, s.store_name, "
        "s.store_street_address, s.store_city, s.store_state, s.store_country, "
        "{data_region_s} AS data_region, s.store_phone, s.first_opened_date, s.last_remodel_date, "
        "s.total_sqft, s.grocery_sqft "
        "FROM {silver_dim_store} s LEFT JOIN {silver_dim_region} r ON s.region_id = r.region_id"
    ),
    "fact_sales": (
        "SELECT t.transaction_date, t.stock_date, t.product_id, t.customer_id, t.store_id, "
        "s.region_id, s.sales_region, s.store_country, s.data_region, t.quantity, "
        "p.product_retail_price AS unit_price, p.product_cost AS unit_cost, "
        "t.quantity * p.product_retail_price AS revenue, t.quantity * p.product_cost AS cost, "
        "t.quantity * (p.product_retail_price - p.product_cost) AS profit "
        "FROM {silver_transactions} t "
        "LEFT JOIN {gold_dim_product} p ON t.product_id = p.product_id "
        "LEFT JOIN {gold_dim_store} s ON t.store_id = s.store_id"
    ),
    "fact_returns": (
        "SELECT r.return_date, r.product_id, r.store_id, s.region_id, s.sales_region, s.store_country, "
        "s.data_region, r.quantity, p.product_retail_price AS unit_price, "
        "r.quantity * p.product_retail_price AS return_value "
        "FROM {silver_returns} r "
        "LEFT JOIN {gold_dim_product} p ON r.product_id = p.product_id "
        "LEFT JOIN {gold_dim_store} s ON r.store_id = s.store_id"
    ),
    "fact_orders": (
        "SELECT o.order_id, o.order_ts, cast(o.order_ts AS DATE) AS order_date, o.customer_id, o.product_id, "
        "o.store_id, s.region_id, s.sales_region, s.store_country, s.data_region, o.quantity, o.unit_price, "
        "o.quantity * o.unit_price AS revenue "
        "FROM {silver_orders} o LEFT JOIN {gold_dim_store} s ON o.store_id = s.store_id"
    ),
    "fact_inventory": (
        "SELECT i.product_id, i.store_id, s.sales_region, s.store_country, s.data_region, "
        "i.quantity_on_hand, i.event_ts FROM ("
        "SELECT product_id, store_id, quantity_on_hand, event_ts FROM ("
        "SELECT *, row_number() OVER (PARTITION BY product_id, store_id ORDER BY event_ts DESC) AS _rn "
        "FROM {silver_inventory}) WHERE _rn = 1) i "
        "LEFT JOIN {gold_dim_store} s ON i.store_id = s.store_id"
    ),
    "agg_sales_monthly": (
        "SELECT date_trunc('month', transaction_date) AS month, year(transaction_date) AS year, "
        "sales_region, store_country, data_region, count(*) AS line_items, sum(quantity) AS units, "
        "sum(revenue) AS revenue, sum(cost) AS cost, sum(profit) AS profit, "
        "count(DISTINCT customer_id) AS customers "
        "FROM {gold_fact_sales} GROUP BY 1, 2, 3, 4, 5"
    ),
    # Per product AND region, so a regional manager never sees another region's totals.
    "agg_product_performance": (
        "SELECT s.product_id, p.product_brand, p.product_name, s.data_region, s.units AS units_sold, "
        "s.revenue, s.profit, coalesce(r.units, 0) AS units_returned, "
        "coalesce(r.units, 0) / nullif(s.units, 0) AS return_rate "
        "FROM (SELECT product_id, data_region, sum(quantity) AS units, sum(revenue) AS revenue, "
        "sum(profit) AS profit FROM {gold_fact_sales} GROUP BY product_id, data_region) s "
        "LEFT JOIN {gold_dim_product} p ON s.product_id = p.product_id "
        "LEFT JOIN (SELECT product_id, data_region, sum(quantity) AS units FROM {gold_fact_returns} "
        "GROUP BY product_id, data_region) r ON s.product_id = r.product_id AND s.data_region = r.data_region"
    ),
    "agg_store_performance": (
        "SELECT st.store_id, st.store_name, st.store_type, st.sales_region, st.store_country, st.data_region, "
        "st.total_sqft, coalesce(s.revenue, 0) AS revenue, coalesce(s.profit, 0) AS profit, "
        "coalesce(s.units, 0) AS units, coalesce(s.revenue, 0) / nullif(st.total_sqft, 0) AS revenue_per_sqft "
        "FROM {gold_dim_store} st "
        "LEFT JOIN (SELECT store_id, sum(revenue) AS revenue, sum(profit) AS profit, sum(quantity) AS units "
        "FROM {gold_fact_sales} GROUP BY store_id) s ON st.store_id = s.store_id"
    ),
    "agg_customer_segments": (
        "SELECT f.data_region, c.member_card, c.yearly_income, c.is_homeowner, c.customer_country, "
        "count(DISTINCT c.customer_id) AS customers, sum(f.quantity) AS units, sum(f.revenue) AS revenue, "
        "sum(f.profit) AS profit "
        "FROM {gold_fact_sales} f JOIN {gold_dim_customer} c ON f.customer_id = c.customer_id "
        "GROUP BY 1, 2, 3, 4, 5"
    ),
    "agg_orders_realtime": (
        "SELECT timestamp_seconds(floor(unix_timestamp(order_ts) / 300) * 300) AS window_start, "
        "data_region, sales_region, count(*) AS orders, sum(quantity) AS units, sum(revenue) AS revenue "
        "FROM {gold_fact_orders} GROUP BY 1, 2, 3"
    ),
    "agg_inventory_turns": (
        "SELECT s.product_id, p.product_brand, p.product_name, s.data_region, s.units AS units_sold, "
        "coalesce(i.on_hand, 0) AS units_on_hand, s.units / nullif(i.on_hand, 0) AS inventory_turns "
        "FROM (SELECT product_id, data_region, sum(quantity) AS units FROM {gold_fact_sales} "
        "GROUP BY product_id, data_region) s "
        "LEFT JOIN {gold_dim_product} p ON s.product_id = p.product_id "
        "LEFT JOIN (SELECT product_id, data_region, sum(quantity_on_hand) AS on_hand FROM {gold_fact_inventory} "
        "GROUP BY product_id, data_region) i ON s.product_id = i.product_id AND s.data_region = i.data_region"
    ),
}
