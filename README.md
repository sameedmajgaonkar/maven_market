# Maven Market

Retail analytics lakehouse on Azure Databricks: ADLS CSV, Confluent Kafka and
MongoDB → Bronze → Silver (cleansing, SCD2) → Gold analytics, with Unity Catalog
governance. Databricks Asset Bundles deploy the pipeline and jobs.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env  # edit local values; never commit credentials
set -a; source .env; set +a
pytest -q
flake8 src tests scripts
```

Runtime code is in `src/`; settings in `config/`; deployment in `databricks.yml`
and `resources/`; governance SQL and policy in `sql/` and `policies/`.

## Deploy

Authenticate with the Databricks CLI, then:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run medallion_refresh -t dev
```

Both targets currently use the same workspace and catalog; coordinate pipeline
ownership before running a second deployment. The `maven_market` secret scope
must contain `confluent-bootstrap`, `confluent-api-key`, `confluent-api-secret`,
and `mongo-url`. Run `governance_bootstrap` for initial governance setup. The
pipeline identity needs the `admin` group and catalog, storage, and tag grants;
`scripts/grant_tag_assign.py` grants tag assignment. Mongo ingestion snapshots
changes; it does not detect deletes.

## Git Flow and CI/CD

| Branch | Purpose / owner |
| --- | --- |
| `main` | Production releases |
| `develop` | Integration |
| `feature/platform-delivery` | Sameed (`sameedmajgaonkar`) |
| `feature/source-ingestion` | Saakshi (`SaakshiD4`) |
| `feature/analytics-models` | Akanksha (`akankshal14`) |
| `release/*` | Stabilize from `develop`; merge into `main` and back into `develop` |
| `hotfix/*` | Fix from `main`; merge into `main` and `develop` |

Create features from `develop` and merge them back with merge commits. Use each
author's repository-local `git config user.name` and `user.email` before committing.
The email must be verified on their GitHub account for contributor attribution.

Actions runs lint, unit tests, and a wheel build on these branches and pull
requests to `main`/`develop`. A successful push to `main` also validates both bundle
targets and deploys prod using GitHub OIDC. Configure repository variable
`DATABRICKS_HOST` and environment `prod` secret `DATABRICKS_CLIENT_ID`; restrict
that environment to `main`. The Databricks `github-cicd` federation must trust this
repository's `prod` environment subject. Source credentials stay in Databricks.
