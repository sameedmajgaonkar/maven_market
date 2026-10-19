"""Bronze layer: raw landing datasets (Lakeflow Declarative Pipeline source).

Every Bronze table is a *streaming table* declared here, so one pipeline owns the whole flow from source
to Gold and is the single writer of each table:

* ADLS Gen2 CSV files          -> Auto Loader (``cloudFiles``)
* Confluent Cloud Kafka        -> Structured Streaming, valid events + a dead-letter table per topic
* MongoDB Atlas master data    -> Python streaming data source (snapshot-on-change), see
                                  ``maven_market.ingestion.mongo_source``

Checkpoints and Auto Loader schema locations are managed by the pipeline itself.

NB: dataset functions must only *define* datasets - no collect/count/save/toTable calls.
"""
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

_src_path = spark.conf.get("maven.src_path")
if _src_path not in sys.path:
    sys.path.append(_src_path)

from maven_market.config import bronze_fqn, get, load_config  # noqa: E402
from maven_market.databricks_utils import get_dbutils  # noqa: E402
from maven_market.ingestion import autoloader, mongo_source  # noqa: E402
from maven_market.ingestion.kafka_stream import kafka_options, split_valid_and_dead  # noqa: E402

cfg = load_config(spark.conf.get("maven.env"), config_dir=spark.conf.get("maven.config_dir"))


def _secret(name: str) -> str:
    return get_dbutils(spark).secrets.get(get(cfg, "secret_scope"), name)


# ------------------------------------------------------------------ 1. CSV Auto Loader
def _define_csv_source(source: str) -> None:
    @dp.table(name=bronze_fqn(cfg, source), comment=f"Raw {source} CSV files ingested with Auto Loader (bronze)")
    def _csv_table():
        return autoloader.read_stream(spark, cfg, source)


for _source in get(cfg, "sources"):
    _define_csv_source(_source)


# ----------------------------------------------------------- 2. Kafka Structured Streaming
def _define_kafka_topic(topic: str, bootstrap: str, key: str, secret: str) -> None:
    t_cfg = get(cfg, f"kafka.topics.{topic}")
    raw_view = f"raw_{topic}_stream"

    @dp.temporary_view(name=raw_view)
    def _raw_stream():
        return spark.readStream.format("kafka").options(**kafka_options(cfg, topic, bootstrap, key, secret)).load()

    @dp.table(name=bronze_fqn(cfg, topic), comment=f"Valid {topic} events from Kafka (bronze)")
    def _valid():
        valid_df, _ = split_valid_and_dead(spark.readStream.table(raw_view), t_cfg["schema"], t_cfg["required"])
        return valid_df

    @dp.table(name=f"{get(cfg, 'catalog')}.{t_cfg['dead_letter_table']}",
              comment=f"Malformed {topic} events from Kafka: unparseable JSON or a missing required key (bronze)")
    def _dead():
        _, dead_df = split_valid_and_dead(spark.readStream.table(raw_view), t_cfg["schema"], t_cfg["required"])
        return dead_df


_kafka_secrets = get(cfg, "kafka.secrets")
for _topic in get(cfg, "kafka.topics"):
    _define_kafka_topic(
        _topic, _secret(_kafka_secrets["bootstrap"]), _secret(_kafka_secrets["api_key"]),
        _secret(_kafka_secrets["api_secret"]),
    )


# --------------------------------------------------------- 3. MongoDB Atlas master data
spark.dataSource.register(mongo_source.build())


def _define_mongo_collection(collection: str, uri: str) -> None:
    spec = get(cfg, f"mongo.collections.{collection}")

    @dp.table(
        name=bronze_fqn(cfg, collection),
        comment=f"MongoDB Atlas {collection}: a snapshot is appended whenever the collection changes (bronze)",
    )
    def _mongo_table():
        return (
            spark.readStream.format(mongo_source.SOURCE_NAME)
            .option("uri", uri)
            .option("database", get(cfg, "mongo.database"))
            .option("collection", collection)
            .option("id_field", spec["id_field"])
            .option("ddl", spec["schema"])
            .option("src_path", _src_path)
            .load()
            .withColumn("_ingested_at", F.current_timestamp())
        )


_mongo_uri = _secret(get(cfg, "mongo.uri_secret"))
for _collection in get(cfg, "mongo.collections"):
    _define_mongo_collection(_collection, _mongo_uri)
