"""Governance as code: apply ``sql/governance/*.sql`` and enforce the gold tagging rule.

* ``split_statements`` / ``render_sql`` turn a ``.sql`` file into executable statements, with the
  catalog and schema names taken from config (``{catalog}``, ``{gold}`` ... placeholders).
* ``find_untagged_tables`` is the pure core of the structural gate: no table in the gold schema
  may exist without the required tags (``owner_team`` and ``data_sensitivity``). A job task runs
  it after every pipeline refresh and fails the run if a table is untagged. Unity Catalog has no
  DDL that blocks ``CREATE TABLE`` on a missing tag, so this gate plus the blocking ABAC policy
  are the enforcement.
"""
from __future__ import annotations

from maven_market.config import get


# --------------------------------------------------------------------------- the tagging plan
# One place that says what is tagged how. Statements are generated from it, so the policies, the
# structural rule and the verification can never drift apart.

# Every gold table must carry both tags (the structural rule). Values must be allowed by the governed tag.
GOLD_TABLE_TAGS = {
    #                         owner_team         data_sensitivity
    "fact_sales":             ("analytics",      "confidential"),
    "fact_returns":           ("analytics",      "confidential"),
    "fact_orders":            ("analytics",      "confidential"),
    "fact_inventory":         ("data_platform",  "internal"),
    "dim_store":              ("data_platform",  "internal"),
    "dim_product":            ("data_platform",  "internal"),
    "dim_date":               ("data_platform",  "public"),
    "dim_customer":           ("compliance",     "restricted"),
    "agg_sales_monthly":      ("regional_sales", "internal"),
    "agg_store_performance":  ("regional_sales", "internal"),
    "agg_product_performance": ("analytics",     "internal"),
    "agg_orders_realtime":    ("data_platform",  "internal"),
    "agg_inventory_turns":    ("data_platform",  "internal"),
    "agg_customer_segments":  ("compliance",     "confidential"),
}

# Columns classified as PII, per table (schema.table). The single mask policy finds them by tag.
_CUSTOMER_PII = [
    "first_name", "last_name", "customer_address", "customer_city", "customer_state_province",
    "customer_postal_code", "customer_acct_num", "birthdate",
]
PII_COLUMNS = {
    "bronze.customers": _CUSTOMER_PII,
    "silver.dim_customer": _CUSTOMER_PII + ["full_name"],
    "gold.dim_customer": _CUSTOMER_PII + ["full_name"],
}
# Financial columns carry the taxonomy tag but are not masked by the PII policy.
FINANCIAL_COLUMNS = {
    "bronze.customers": ["yearly_income"],
    "silver.dim_customer": ["yearly_income"],
    "gold.dim_customer": ["yearly_income"],
    "gold.agg_customer_segments": ["yearly_income"],
}
# Gold tables whose rows each belong to a region: the column holding it is marked `region_code`.
REGION_COLUMN_TABLES = [
    "fact_sales", "fact_returns", "fact_orders", "fact_inventory", "dim_store", "agg_sales_monthly",
    "agg_store_performance", "agg_orders_realtime", "agg_product_performance", "agg_customer_segments",
    "agg_inventory_turns",
]
REGION_COLUMN = "data_region"
REQUIRED_GOLD_TAGS = ["owner_team", "data_sensitivity"]
# Every governed tag key the tagging plan writes. The identity that runs the job needs ASSIGN on each.
GOVERNED_TAGS_SET_BY_JOB = ["owner_team", "data_sensitivity", "region_code", "classification"]


def tag_assignment_statements(cfg: dict) -> list[str]:
    """``SET TAG`` statements for the whole plan (idempotent: re-running just re-sets the same values)."""
    catalog = get(cfg, "catalog")
    gold = get(cfg, "schemas.gold")
    out = []
    for table, (owner, sensitivity) in GOLD_TABLE_TAGS.items():
        out.append(f"SET TAG ON TABLE {catalog}.{gold}.{table} `owner_team` = `{owner}`")
        out.append(f"SET TAG ON TABLE {catalog}.{gold}.{table} `data_sensitivity` = `{sensitivity}`")
    for table in REGION_COLUMN_TABLES:
        out.append(f"SET TAG ON COLUMN {catalog}.{gold}.{table}.{REGION_COLUMN} `region_code`")
    for qualified, columns in PII_COLUMNS.items():
        schema, table = qualified.split(".")
        schema = get(cfg, f"schemas.{schema}")
        for column in columns:
            out.append(f"SET TAG ON COLUMN {catalog}.{schema}.{table}.{column} `classification` = `PII`")
    for qualified, columns in FINANCIAL_COLUMNS.items():
        schema, table = qualified.split(".")
        schema = get(cfg, f"schemas.{schema}")
        for column in columns:
            out.append(f"SET TAG ON COLUMN {catalog}.{schema}.{table}.{column} `classification` = `Financial`")
    return out


def governance_placeholders(cfg: dict) -> dict:
    """Names usable as ``{placeholder}`` in governance SQL, all taken from config."""
    schemas = get(cfg, "schemas")
    return {"catalog": get(cfg, "catalog"), **{name: value for name, value in schemas.items()}}


def render_sql(sql: str, cfg: dict) -> str:
    return sql.format(**governance_placeholders(cfg))


def split_statements(sql: str) -> list[str]:
    """Split SQL text on top-level semicolons.

    Semicolons inside ``'...'`` / ``"..."`` literals, backtick identifiers, ``--`` line comments and
    ``/* */`` block comments do not split. Comments are dropped; empty statements are skipped.
    """
    statements: list[str] = []
    current: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":                      # line comment: skip to end of line
            while i < n and sql[i] != "\n":
                i += 1
            continue
        if ch == "/" and nxt == "*":                      # block comment
            i += 2
            while i < n and not (sql[i] == "*" and i + 1 < n and sql[i + 1] == "/"):
                i += 1
            i += 2
            continue
        if ch in ("'", '"', "`"):                         # quoted section: copy verbatim
            quote = ch
            current.append(ch)
            i += 1
            while i < n:
                current.append(sql[i])
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:  # doubled quote = escaped quote
                        current.append(sql[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def find_untagged_tables(tables: list[str], tagged: set[tuple[str, str]], required: list[str]) -> dict[str, list[str]]:
    """``{table: [missing required tags]}`` for every table lacking at least one required tag.

    ``tagged`` is a set of ``(table_name, tag_name)`` pairs, e.g. read from ``information_schema.table_tags``.
    """
    missing = {}
    for table in tables:
        absent = [tag for tag in required if (table, tag) not in tagged]
        if absent:
            missing[table] = absent
    return missing
