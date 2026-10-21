"""Guards the cluster policy against drifting from the Global Constraints in the plan."""
import json
from pathlib import Path

POLICY = Path(__file__).resolve().parents[1] / "policies" / "maven_market_cluster_policy.json"
ALLOWED_VMS = {"Standard_F4", "Standard_F8", "Standard_D4ds_v5", "Standard_D8ds_v5"}


def _policy():
    return json.loads(POLICY.read_text())


def test_vm_allowlist_is_exactly_f_and_d_series():
    p = _policy()
    for key in ("node_type_id", "driver_node_type_id"):
        assert p[key]["type"] == "allowlist"
        assert set(p[key]["values"]) == ALLOWED_VMS
        assert p[key]["defaultValue"] in ALLOWED_VMS


def test_workers_capped_at_two():
    w = _policy()["num_workers"]
    assert (w["minValue"], w["maxValue"]) == (0, 2)


def test_autotermination_capped_at_30():
    a = _policy()["autotermination_minutes"]
    assert a["maxValue"] <= 30
    assert a["defaultValue"] <= a["maxValue"]


def test_project_tag_is_fixed():
    t = _policy()["custom_tags.project"]
    assert t == {"type": "fixed", "value": "maven_market"}
