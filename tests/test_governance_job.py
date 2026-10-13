"""Pure-Python tests for the governance job entry point and the governance SQL files."""
from pathlib import Path

import pytest

from maven_market.config import load_config
from maven_market.governance import render_sql, split_statements
from maven_market.jobs.governance import (
    POLICY_FILES,
    parse_args,
    plan_actions,
    resolve_sql_dir,
    untagged_gold_tables,
)

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "governance"


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def test_args_default_to_the_routine_actions():
    a = parse_args([])
    assert a.env == "dev" and a.config_dir is None and a.action == "refresh"


def test_plan_actions_refresh_is_tags_then_gate():
    # the routine post-pipeline run: (re)apply tags, then fail if anything is still untagged
    assert plan_actions("refresh") == ["tags", "gate"]


def test_plan_actions_bootstrap_applies_everything_in_a_safe_order():
    steps = plan_actions("bootstrap")
    assert steps == ["policies", "tags", "gate"]
    assert steps.index("tags") < steps.index("gate")


def test_plan_actions_single_and_unknown():
    assert plan_actions("gate") == ["gate"] and plan_actions("tags") == ["tags"]
    with pytest.raises(ValueError):
        plan_actions("drop-everything")


def test_policy_files_exist_and_are_applied_in_dependency_order():
    for name in POLICY_FILES:
        assert (SQL_DIR / name).exists(), f"{name} missing"
    order = list(POLICY_FILES)
    assert order.index("grants.sql") < order.index("abac_policies.sql")
    assert order.index("row_filters.sql") < order.index("structural_rules.sql")
    # governed_tags.sql is account-level (CREATE GOVERNED TAG is not re-runnable): never part of the job
    assert "governed_tags.sql" not in order and "catalog_schemas.sql" not in order


@pytest.mark.parametrize("name", POLICY_FILES)
def test_every_policy_file_renders_and_splits_cleanly(cfg, name):
    rendered = render_sql((SQL_DIR / name).read_text(), cfg)   # KeyError here = a stray {brace} in the file
    statements = split_statements(rendered)
    assert statements, f"{name} produced no statements"
    assert not any("{" in s or "}" in s for s in statements)


def test_exemptions_are_consistent_across_all_policies(cfg):
    """Every ABAC policy must exempt the same principals, or one of them would filter/mask the pipeline."""
    text = "\n".join(render_sql((SQL_DIR / n).read_text(), cfg) for n in POLICY_FILES)
    statements = [s for s in split_statements(text) if s.upper().startswith("CREATE OR REPLACE POLICY")]
    assert len(statements) == 3
    for s in statements:
        assert "EXCEPT `admin`, `data_compliance`" in s, "policy must exempt admin and data_compliance"


def test_region_and_block_policies_are_mutually_exclusive(cfg):
    """Two different row filters on one table is a runtime ERROR; the WHEN clauses must be exact opposites."""
    text = render_sql((SQL_DIR / "row_filters.sql").read_text(), cfg) + render_sql(
        (SQL_DIR / "structural_rules.sql").read_text(), cfg)
    assert "WHEN has_tag('owner_team') AND has_tag('data_sensitivity')" in text
    assert "WHEN NOT (has_tag('owner_team') AND has_tag('data_sensitivity'))" in text


def test_only_one_column_mask_policy_exists(cfg):
    """PDF: a SINGLE tag-based mask policy. Several would conflict whenever they match the same column."""
    text = "\n".join(render_sql((SQL_DIR / n).read_text(), cfg) for n in POLICY_FILES)
    assert text.count("COLUMN MASK") == 1


def test_mask_policy_matches_by_tag_never_by_column_name(cfg):
    sql = render_sql((SQL_DIR / "abac_policies.sql").read_text(), cfg)
    assert "has_tag_value('classification', 'PII')" in sql
    for column in ("first_name", "last_name", "birthdate", "customer_address"):
        assert column not in sql, f"mask policy must not hardcode {column}"


def test_sql_dir_is_derived_from_the_config_dir_unless_given():
    assert resolve_sql_dir("/ws/files/config", None) == Path("/ws/files/sql/governance")
    assert resolve_sql_dir("/ws/files/config", "/x") == Path("/x")
    assert resolve_sql_dir(None, None) == SQL_DIR


def test_gate_ignores_hidden_materialization_tables_but_flags_real_gaps():
    tables = ["fact_sales", "new_table", "__materialization_mat_abc_fact_sales_1"]
    tags = [("fact_sales", "owner_team"), ("fact_sales", "data_sensitivity"), ("fact_sales", "extra")]
    assert untagged_gold_tables(tables, tags) == {"new_table": ["owner_team", "data_sensitivity"]}


def test_no_real_logins_are_committed_in_governance_sql():
    for path in SQL_DIR.glob("*.sql"):
        if path.name.endswith(".seed.sql"):   # the git-ignored local file that legitimately holds them
            continue
        assert "@gmail" not in path.read_text().lower(), f"{path.name} contains a personal email"


def test_mapping_table_has_the_name_and_columns_the_pdf_requires(cfg):
    sql = render_sql((SQL_DIR / "row_filters.sql").read_text(), cfg)
    assert "maven_market_uc.audit.user_region_mapping" in sql
    assert "user_email STRING" in sql and "region     STRING" in sql
