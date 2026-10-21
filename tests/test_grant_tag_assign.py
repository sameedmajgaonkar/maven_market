"""The governed-tag ASSIGN grant: a safe merge into an authoritative rule set, and tag coverage."""
import importlib.util
import re
from pathlib import Path

from maven_market.config import load_config
from maven_market.governance import GOVERNED_TAGS_SET_BY_JOB, tag_assignment_statements

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "grant_tag_assign.py"
_spec = importlib.util.spec_from_file_location("grant_tag_assign", _SCRIPT)
grant_tag_assign = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(grant_tag_assign)

MANAGER = {"role": "roles/tagPolicy.manager", "principals": ["users/steward@example.com"]}
SP = "servicePrincipals/abc"


def test_adds_a_new_assigner_rule_and_keeps_the_manager():
    rules = grant_tag_assign.with_assigner([MANAGER], SP)
    assert MANAGER in rules
    assert {"role": "roles/tagPolicy.assigner", "principals": [SP]} in rules


def test_appends_to_an_existing_assigner_rule_without_dropping_anyone():
    existing = {"role": "roles/tagPolicy.assigner", "principals": ["groups/stewards"]}
    rules = grant_tag_assign.with_assigner([MANAGER, existing], SP)
    assigner = next(r for r in rules if r["role"] == "roles/tagPolicy.assigner")
    assert assigner["principals"] == ["groups/stewards", SP]
    assert MANAGER in rules


def test_is_idempotent():
    once = grant_tag_assign.with_assigner([MANAGER], SP)
    assert grant_tag_assign.with_assigner(once, SP) == once


def test_does_not_mutate_its_input():
    original = [{"role": "roles/tagPolicy.assigner", "principals": ["groups/stewards"]}]
    grant_tag_assign.with_assigner(original, SP)
    assert original == [{"role": "roles/tagPolicy.assigner", "principals": ["groups/stewards"]}]


def test_every_tag_the_plan_sets_is_covered_by_the_grant_list():
    """Otherwise the prod identity could be missing ASSIGN on a tag the job writes, and only fail in prod."""
    statements = tag_assignment_statements(load_config("dev"))
    # a tag KEY is the first backticked token after the object name (values are backticked too)
    keys = {re.search(r"SET TAG ON \w+ \S+ `([a-z_]+)`", s).group(1) for s in statements}
    assert keys == set(GOVERNED_TAGS_SET_BY_JOB)
