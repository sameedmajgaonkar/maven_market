"""Contract tests for the end-to-end pipeline: Bronze is declared in the pipeline, which is the single writer.

The Spark parts of ``pipelines/bronze.py`` need a pipeline runtime, so these tests pin everything around it:
the config it reads, the bundle wiring, and the lineage from every Bronze dataset into Silver and Gold.
"""
from pathlib import Path

import pytest
import yaml

from maven_market.config import audit_table, bronze_fqn, load_config
from maven_market.ingestion import mongo_source
from maven_market.ingestion.autoloader import autoloader_options, source_path
from maven_market.ingestion.kafka_stream import kafka_options, topic_settings
from maven_market.ingestion.mongo_ingest import schema_columns, schema_types
from maven_market.pipelines import transforms as T

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def cfg():
    return load_config("dev")


def _bronze_targets(cfg):
    catalog = cfg["catalog"]
    targets = [bronze_fqn(cfg, n) for n in cfg["sources"]]
    targets += [bronze_fqn(cfg, n) for n in cfg["mongo"]["collections"]]
    for name, spec in cfg["kafka"]["topics"].items():
        targets += [bronze_fqn(cfg, name), f"{catalog}.{spec['dead_letter_table']}"]
    return targets


# ------------------------------------------------------------------ source coverage
def test_all_three_source_types_are_configured(cfg):
    assert {"stores", "regions", "returns", "calendar", "transactions"} <= set(cfg["sources"])
    assert {"orders", "inventory"} <= set(cfg["kafka"]["topics"])
    assert set(cfg["mongo"]["collections"]) == {"products", "customers"}


def test_every_bronze_table_has_a_single_writer(cfg):
    targets = _bronze_targets(cfg)
    assert len(targets) == len(set(targets)), f"duplicate Bronze targets: {targets}"


def test_no_job_ingests_into_bronze_the_pipeline_is_the_only_writer():
    jobs = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())["resources"]["jobs"]
    entry_points = {
        t["python_wheel_task"]["entry_point"]
        for job in jobs.values() for t in job["tasks"] if "python_wheel_task" in t
    }
    assert not entry_points & {"bronze_csv", "bronze_kafka", "bronze_mongo"}
    scripts = (ROOT / "pyproject.toml").read_text()
    assert "bronze_csv" not in scripts and "bronze_kafka" not in scripts and "bronze_mongo" not in scripts


def test_pipeline_declares_bronze_silver_and_gold_in_one_pipeline():
    pipelines = yaml.safe_load((ROOT / "resources" / "pipelines.yml").read_text())["resources"]["pipelines"]
    assert set(pipelines) == {"silver_gold"}
    paths = [lib["file"]["path"] for lib in pipelines["silver_gold"]["libraries"]]
    assert [Path(p).name for p in paths] == ["bronze.py", "silver.py", "gold.py"]
    # the Mongo reader imports pymongo on the pipeline compute
    assert any(d.startswith("pymongo==") for d in pipelines["silver_gold"]["environment"]["dependencies"])


def test_medallion_refresh_is_one_pipeline_update_followed_by_governance():
    jobs = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())["resources"]["jobs"]
    tasks = {t["task_key"]: t for t in jobs["medallion_refresh"]["tasks"]}
    assert "pipeline_task" in tasks["refresh_pipeline"]
    assert tasks["govern"]["depends_on"] == [{"task_key": "refresh_pipeline"}]
    # the audit task must also run (and record the error) when the pipeline update failed
    assert tasks["audit"]["depends_on"] == [{"task_key": "refresh_pipeline"}]
    assert tasks["audit"]["run_if"] == "ALL_DONE"
    assert tasks["audit"]["python_wheel_task"]["entry_point"] == "pipeline_audit"


# ------------------------------------------------------------------ ingestion contracts
def test_autoloader_contract_for_all_csv_sources(cfg):
    for source in cfg["sources"]:
        opts = autoloader_options(cfg, source)
        assert opts["cloudFiles.format"] == "csv"
        assert opts["cloudFiles.inferColumnTypes"] == "true"
        assert opts["cloudFiles.schemaEvolutionMode"] == "rescue"
        assert opts["rescuedDataColumn"] == "_rescued_data"
        assert source_path(cfg, source).startswith(cfg["storage"]["landing_root"])


def test_kafka_contract_for_all_streaming_topics(cfg):
    for topic in cfg["kafka"]["topics"]:
        opts = kafka_options(cfg, topic, "host:9092", "key", "secret")
        assert opts["subscribe"] == topic
        assert opts["kafka.security.protocol"] == "SASL_SSL"
        spec = topic_settings(cfg, topic)
        assert spec["dead_letter_table"].endswith("_dead_letter") and spec["required"]


def test_mongo_collections_have_what_the_stream_needs(cfg):
    assert mongo_source.SOURCE_NAME == "maven_mongo"
    for name, spec in cfg["mongo"]["collections"].items():
        cols = schema_columns(spec["schema"])
        assert "_id" in cols and spec["id_field"] in cols, name
        assert all(t in {"INT", "LONG", "DOUBLE", "STRING", "DATE"} for t in schema_types(spec["schema"]).values())
    assert "method" not in cfg["mongo"]  # the Spark connector cannot run inside a pipeline


def test_connection_secrets_are_names_never_values(cfg):
    assert cfg["mongo"]["uri_secret"] == "mongo-url"
    assert "mongodb+srv" not in str(cfg)


def test_audit_table_is_fully_qualified_from_config(cfg):
    assert audit_table(cfg) == "maven_market_uc.audit.audit_logs"


# ------------------------------------------------------------------ lineage Bronze -> Silver -> Gold
def test_every_bronze_dataset_feeds_silver(cfg):
    feeds = " ".join(T.SILVER_SQL.values()) + " ".join(
        f"{{bronze_{spec['bronze']}}}" for spec in list(T.SCD2_SOURCES.values()) + list(T.SILVER_STREAMS.values())
    )
    for group in ("sources", "mongo.collections"):
        for dataset in (cfg["sources"] if group == "sources" else cfg["mongo"]["collections"]):
            assert f"{{bronze_{dataset}}}" in feeds, f"bronze.{dataset} does not feed Silver"
    for topic in cfg["kafka"]["topics"]:
        assert f"{{bronze_{topic}}}" in feeds


def test_master_data_is_scd2_from_mongo():
    assert T.SCD2_SOURCES["dim_customer"]["bronze"] == "customers"
    assert T.SCD2_SOURCES["dim_product"]["bronze"] == "products"
    assert "dim_product" not in T.SILVER_SQL


def test_silver_feeds_gold():
    assert "{silver_dim_product}" in T.GOLD_SQL["dim_product"]
    assert "{silver_dim_customer}" in T.GOLD_SQL["dim_customer"]
    assert "{silver_transactions}" in T.GOLD_SQL["fact_sales"]
    assert "{silver_orders}" in T.GOLD_SQL["fact_orders"]
    assert "{silver_inventory}" in T.GOLD_SQL["fact_inventory"]
