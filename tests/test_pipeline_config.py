"""Config-driven DQ rules and table names for the Silver/Gold pipeline (pure Python, no Spark)."""
import pytest

from maven_market.config import load_config, load_expectations, table_fqn
from maven_market.pipelines.common import expectation_groups

VALID_ACTIONS = {"warn", "drop", "fail"}


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


@pytest.fixture(scope="module")
def exp():
    return load_expectations()


def test_table_fqn_qualifies_with_catalog(cfg):
    assert table_fqn(cfg, "silver", "dim_customer") == "maven_market_uc.silver.dim_customer"
    assert table_fqn(cfg, "gold", "fact_sales") == "maven_market_uc.gold.fact_sales"


def test_table_fqn_unknown_key_raises(cfg):
    with pytest.raises(KeyError):
        table_fqn(cfg, "gold", "nope")


def test_every_expectation_group_targets_a_configured_silver_table(cfg, exp):
    silver = set(cfg["pipeline"]["silver"])
    assert set(exp) <= silver, f"expectations for unknown tables: {set(exp) - silver}"


def test_expectation_actions_are_valid_and_constraints_are_non_empty_strings(exp):
    for table, groups in exp.items():
        assert set(groups) <= VALID_ACTIONS, f"{table}: bad action(s) {set(groups) - VALID_ACTIONS}"
        for action, rules in groups.items():
            for name, constraint in (rules or {}).items():
                assert isinstance(constraint, str) and constraint.strip(), f"{table}.{action}.{name}"


def test_critical_keys_fail_the_update(exp):
    # plan T7.1: expect_or_fail for critical keys
    assert exp["transactions"]["fail"], "transactions must fail the update on null foreign keys"


def test_expectation_groups_returns_three_dicts_even_when_absent():
    assert expectation_groups({}, "dim_store") == ({}, {}, {})


def test_expectation_groups_splits_by_action():
    exp = {"t": {"warn": {"a": "x > 0"}, "drop": {"b": "y IS NOT NULL"}}}
    warn, drop, fail = expectation_groups(exp, "t")
    assert warn == {"a": "x > 0"} and drop == {"b": "y IS NOT NULL"} and fail == {}


def test_load_expectations_reads_given_dir(tmp_path):
    (tmp_path / "expectations.yml").write_text("t:\n  drop:\n    r: \"a IS NOT NULL\"\n")
    assert load_expectations(tmp_path) == {"t": {"drop": {"r": "a IS NOT NULL"}}}


def test_load_expectations_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_expectations(tmp_path)


def test_changing_a_threshold_in_config_changes_the_rules_no_code_edit(tmp_path):
    # plan T7.5: DQ behaviour is config, not code
    path = tmp_path / "expectations.yml"
    path.write_text("t:\n  drop:\n    positive_qty: \"quantity > 0\"\n")
    before = expectation_groups(load_expectations(tmp_path), "t")[1]
    path.write_text("t:\n  drop:\n    positive_qty: \"quantity > 5\"\n")
    after = expectation_groups(load_expectations(tmp_path), "t")[1]
    assert before != after and after["positive_qty"] == "quantity > 5"
