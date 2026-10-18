"""Helpers shared by the Silver and Gold pipeline definitions."""
from __future__ import annotations


def expectation_groups(expectations: dict, table: str) -> tuple[dict, dict, dict]:
    """Return ``(warn, drop, fail)`` rule dicts for ``table`` (each ``{description: constraint}``).

    A table with no rules yields three empty dicts.
    """
    rules = expectations.get(table) or {}
    return (dict(rules.get("warn") or {}), dict(rules.get("drop") or {}), dict(rules.get("fail") or {}))


def with_expectations(expectations: dict, table: str):  # pragma: no cover - needs a pipeline runtime
    """Decorator applying the config-defined expectations to a dataset function.

    Order of use: ``@dp.materialized_view(...)`` on the outside, this on the inside, because
    expectation decorators must sit between the dataset decorator and the function.
    """
    from pyspark import pipelines as dp

    warn, drop, fail = expectation_groups(expectations, table)

    def decorate(fn):
        if fail:
            fn = dp.expect_all_or_fail(fail)(fn)
        if drop:
            fn = dp.expect_all_or_drop(drop)(fn)
        if warn:
            fn = dp.expect_all(warn)(fn)
        return fn

    return decorate
