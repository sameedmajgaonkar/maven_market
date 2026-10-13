"""Grant a principal ASSIGN on the governed tags the governance job sets.

Governed-tag permissions are not SQL GRANTs: they live in account access-control rule sets, one per
tag. The prod pipeline identity (the ``github-cicd`` service principal) needs ASSIGN on every tag the
governance job writes, otherwise ``SET TAG`` fails there even though it works for the tag manager.

    python scripts/grant_tag_assign.py --profile <profile> --account-id <id> \\
        --principal servicePrincipals/<application-id>

Run it as a tag manager (the creator of the tags). Idempotent, and it only ADDS the principal: rule sets
are authoritative on the server, so the existing grants are read first and written back unchanged.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from maven_market.governance import GOVERNED_TAGS_SET_BY_JOB  # noqa: E402

ASSIGNER_ROLE = "roles/tagPolicy.assigner"


def with_assigner(grant_rules: list[dict], principal: str) -> list[dict]:
    """``grant_rules`` plus ``principal`` as an assigner. Other rules and principals are untouched."""
    rules = [{**r, "principals": list(r.get("principals", []))} for r in grant_rules]
    for rule in rules:
        if rule["role"] == ASSIGNER_ROLE:
            if principal not in rule["principals"]:
                rule["principals"].append(principal)
            return rules
    return rules + [{"role": ASSIGNER_ROLE, "principals": [principal]}]


def _databricks(profile: str, *args: str) -> dict:
    proc = subprocess.run(["databricks", *args, "-p", profile], capture_output=True, text=True)
    out = "\n".join(line for line in proc.stdout.splitlines() if not line.startswith("Warn"))
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        raise RuntimeError((proc.stderr or out)[:400]) from None


def grant(profile: str, account_id: str, principal: str, tag_key: str) -> list[dict]:
    policy = _databricks(profile, "tag-policies", "get-tag-policy", tag_key, "-o", "json")
    name = f"accounts/{account_id}/tagPolicies/{policy['id']}/ruleSets/default"
    url = f"/api/2.0/preview/accounts/access-control/rule-sets?name={name}&etag="
    current = _databricks(profile, "api", "get", url)
    rules = with_assigner(current.get("grant_rules", []), principal)
    body = {"name": name, "rule_set": {"name": name, "etag": current.get("etag", ""), "grant_rules": rules}}
    updated = _databricks(profile, "api", "put", "/api/2.0/preview/accounts/access-control/rule-sets",
                          "--json", json.dumps(body))
    return updated.get("grant_rules", [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--account-id", required=True)
    ap.add_argument("--principal", required=True, help="e.g. servicePrincipals/<application-id> or groups/<name>")
    args = ap.parse_args()
    for tag in GOVERNED_TAGS_SET_BY_JOB:
        rules = grant(args.profile, args.account_id, args.principal, tag)
        ok = any(r["role"] == ASSIGNER_ROLE and args.principal in r.get("principals", []) for r in rules)
        print(f"{'ok  ' if ok else 'FAIL'} {tag}")
        if not ok:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
