"""Job entry point: apply governance (policies, tags) and enforce the gold tagging rule.

Run as a Databricks python_wheel_task (``governance`` console script).

Actions (``--action``):

* ``refresh``   (default) re-apply the tags, then run the gate. Used after every pipeline refresh: a
                full refresh can recreate a table and drop its tags, and the gate then fails the run.
* ``bootstrap`` create/replace the ABAC policies, then tags, then the gate (first-time setup).
* ``policies`` / ``tags`` / ``gate``  a single step.

``governed_tags.sql`` (CREATE GOVERNED TAG) and ``catalog_schemas.sql`` are account/metastore-level and
not re-runnable, so they are never part of the job.

The run-as identity must be a member of the exempt ``admin`` group; otherwise the policies would mask or
filter the very data the pipelines read and write.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from maven_market.config import audit_table, get, load_config
from maven_market.governance import (
    REQUIRED_GOLD_TAGS,
    find_untagged_tables,
    render_sql,
    split_statements,
    tag_assignment_statements,
)
from maven_market.logging_utils import AuditLogger

# Dependency order: grants before the policies that rely on them; the row-filter function and mapping
# table before the structural rule that sits next to the region filter.
POLICY_FILES = ("grants.sql", "abac_policies.sql", "row_filters.sql", "structural_rules.sql")

_PLANS = {
    "refresh": ["tags", "gate"],
    "bootstrap": ["policies", "tags", "gate"],
    "policies": ["policies"],
    "tags": ["tags"],
    "gate": ["gate"],
}


def plan_actions(action: str) -> list[str]:
    """The ordered steps an ``--action`` stands for."""
    if action not in _PLANS:
        raise ValueError(f"Unknown action '{action}'; expected one of {sorted(_PLANS)}")
    return list(_PLANS[action])


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apply governance and enforce the gold tagging rule")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument("--action", default="refresh", choices=sorted(_PLANS))
    p.add_argument("--sql-dir", default=None, help="Default: <config-dir>/../sql/governance")
    return p.parse_args(argv)


def resolve_sql_dir(config_dir: str | None, sql_dir: str | None) -> Path:
    if sql_dir:
        return Path(sql_dir)
    root = Path(config_dir).parent if config_dir else Path(__file__).resolve().parents[3]
    return root / "sql" / "governance"


def untagged_gold_tables(table_names: list[str], tag_rows: list[tuple[str, str]]) -> dict[str, list[str]]:
    """``{table: [missing tags]}`` for the user-facing gold tables.

    Names starting with ``__`` are Databricks' hidden materialization tables behind materialized
    views, not datasets anyone queries, so they are not part of the rule.
    """
    visible = [t for t in table_names if not t.startswith("__")]
    return find_untagged_tables(visible, set(tag_rows), REQUIRED_GOLD_TAGS)


def _run(spark, statements: list[str]) -> int:  # pragma: no cover - needs Spark
    for statement in statements:
        spark.sql(statement)
    return len(statements)


def apply_policies(spark, cfg: dict, sql_dir: Path, audit: AuditLogger) -> None:  # pragma: no cover
    for name in POLICY_FILES:
        statements = split_statements(render_sql((sql_dir / name).read_text(), cfg))
        with audit.track("governance", f"policies:{name}") as m:
            m["rows_out"] = _run(spark, statements)


def apply_tags(spark, cfg: dict, audit: AuditLogger) -> None:  # pragma: no cover
    with audit.track("governance", "tags") as m:
        m["rows_out"] = _run(spark, tag_assignment_statements(cfg))


def check_gate(spark, cfg: dict, audit: AuditLogger) -> None:  # pragma: no cover
    catalog, gold = get(cfg, "catalog"), get(cfg, "schemas.gold")
    with audit.track("governance", "gold_tag_gate") as m:
        tables = [r["table_name"] for r in spark.sql(
            f"SELECT table_name FROM {catalog}.information_schema.tables WHERE table_schema = '{gold}'").collect()]
        tags = [(r["table_name"], r["tag_name"]) for r in spark.sql(
            f"SELECT table_name, tag_name FROM {catalog}.information_schema.table_tags "
            f"WHERE schema_name = '{gold}'").collect()]
        missing = untagged_gold_tables(tables, tags)
        m["rows_in"], m["rows_rejected"] = len(tables), len(missing)
        if missing:
            detail = "; ".join(f"{t}: missing {', '.join(tags_)}" for t, tags_ in sorted(missing.items()))
            raise RuntimeError(f"Gold tables without required governance tags ({detail})")


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Databricks
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)
    sql_dir = resolve_sql_dir(args.config_dir, args.sql_dir)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    audit = AuditLogger(table=audit_table(cfg))
    for step in plan_actions(args.action):
        if step == "policies":
            apply_policies(spark, cfg, sql_dir, audit)
        elif step == "tags":
            apply_tags(spark, cfg, audit)
        else:
            check_gate(spark, cfg, audit)


if __name__ == "__main__":  # pragma: no cover
    main()
