"""Confluent Kafka -> Bronze via Structured Streaming, with a dead-letter path.

Builds the Kafka reader options and splits each event into the valid / dead-letter stream used by the
Bronze streaming tables of the pipeline. Malformed events (bad JSON, or missing a configured required key)
go to the topic's dead-letter table instead of failing the stream (Review Focus #1).
"""
from __future__ import annotations

from maven_market.config import get

# Databricks ships a SHADED Kafka client. The unshaded login module name is rejected with
# RESTRICTED_STREAMING_OPTION, so this exact prefix matters.
_JAAS_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"


def topic_settings(cfg: dict, topic: str) -> dict:
    return get(cfg, f"kafka.topics.{topic}")


def kafka_options(cfg: dict, topic: str, bootstrap: str, api_key: str, api_secret: str) -> dict:
    topic_settings(cfg, topic)  # fail fast on unknown topic
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            f'{_JAAS_MODULE} required username="{api_key}" password="{api_secret}";'
        ),
        "startingOffsets": get(cfg, "kafka.starting_offsets"),
    }


def split_valid_and_dead(raw_df, schema: str, required: list[str]):  # pragma: no cover - needs Spark
    """Parse JSON values; return ``(valid_df, dead_letter_df)``.

    ``from_json`` yields a NULL struct for unparseable JSON, and individual NULL fields for
    missing ones, so both failure modes are caught by the same check.
    """
    from pyspark.sql import functions as F

    df = raw_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    ).withColumn("parsed", F.from_json("raw_value", schema))

    is_valid = F.col("parsed").isNotNull()
    for key in required:
        is_valid = is_valid & F.col(f"parsed.{key}").isNotNull()

    meta = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"]
    valid = df.filter(is_valid).select("parsed.*", "raw_value", *meta, F.current_timestamp().alias("_ingested_at"))
    dead = df.filter(~is_valid).select(
        "raw_value", *meta,
        F.when(F.col("parsed").isNull(), F.lit("unparseable_json"))
        .otherwise(F.lit("missing_required_key")).alias("_error"),
        F.current_timestamp().alias("_ingested_at"),
    )
    return valid, dead
