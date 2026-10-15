"""CSV -> Bronze via Auto Loader (``cloudFiles``), for the Bronze streaming tables of the pipeline.

Every path and option comes from config; nothing is hardcoded here. The option builder is a pure function
(unit-tested without Spark); ``read_stream`` is a thin Spark wrapper. The pipeline owns the checkpoint and
the inferred-schema location, so neither is configured here.
"""
from __future__ import annotations

from maven_market.config import get


def _landing(cfg: dict) -> str:
    return get(cfg, "storage.landing_root").rstrip("/")


def source_path(cfg: dict, source: str) -> str:
    return f"{_landing(cfg)}/{get(cfg, f'sources.{source}.path')}"


def autoloader_options(cfg: dict, source: str) -> dict:
    get(cfg, f"sources.{source}")  # fail fast on unknown source
    return {
        "cloudFiles.format": get(cfg, f"sources.{source}.format"),
        # without this every CSV column is a string
        "cloudFiles.inferColumnTypes": str(get(cfg, "autoloader.infer_column_types")).lower(),
        # new/renamed columns are captured in the rescued column instead of failing the stream
        "cloudFiles.schemaEvolutionMode": get(cfg, "autoloader.schema_evolution_mode"),
        # NOTE: this option has no 'cloudFiles.' prefix
        "rescuedDataColumn": get(cfg, "autoloader.rescued_data_column"),
        "header": "true",
    }


def read_stream(spark, cfg: dict, source: str):  # pragma: no cover - needs Spark
    from pyspark.sql import functions as F

    df = (
        spark.readStream.format("cloudFiles")
        .options(**autoloader_options(cfg, source))
        .load(source_path(cfg, source))
    )
    # `_metadata` is a hidden column: it must be selected explicitly to be kept for lineage.
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file_path"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )
