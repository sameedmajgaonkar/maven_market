"""Silver layer: cleansed, typed, deduplicated datasets (Lakeflow Declarative Pipeline source).

Everything environment-specific comes from config: the pipeline passes ``maven.env``,
``maven.config_dir`` and ``maven.src_path`` as pipeline settings (see resources/pipelines.yml).
Transform logic lives in ``maven_market.pipelines.transforms``; data-quality rules in
``config/expectations.yml``.

NB: dataset functions must only *define* datasets - no collect/count/save/toTable calls.
"""
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

_src_path = spark.conf.get("maven.src_path")
if _src_path not in sys.path:
    sys.path.append(_src_path)

from maven_market.config import load_config, load_expectations, table_fqn  # noqa: E402
from maven_market.pipelines import transforms as T  # noqa: E402
from maven_market.pipelines.common import with_expectations  # noqa: E402

_config_dir = spark.conf.get("maven.config_dir")
cfg = load_config(spark.conf.get("maven.env"), config_dir=_config_dir)
EXPECTATIONS = load_expectations(_config_dir)
NAMES = T.names(cfg)


def _define_scd2_dimension(dim: str, spec: dict) -> None:
    """SCD Type 2 dimension: Bronze changes -> cleansed view -> auto-CDC flow with full history."""
    source_view = f"{dim}_changes"
    target = table_fqn(cfg, "silver", dim)
    bronze = NAMES[f"bronze_{spec['bronze']}"]

    @dp.temporary_view(name=source_view)
    @with_expectations(EXPECTATIONS, dim)
    def _changes():
        return spark.readStream.table(bronze).selectExpr(*spec["exprs"])

    dp.create_streaming_table(name=target, comment=f"SCD2 history of {spec['bronze']} (silver)")
    dp.create_auto_cdc_flow(
        target=target,
        source=source_view,
        keys=spec["keys"],
        # out-of-order / duplicate change events are resolved by this ordering column
        sequence_by=spec["sequence_by"],
        stored_as_scd_type="2",
        # lineage columns changing (e.g. a re-delivered file) must not create a new version
        track_history_except_column_list=T.scd2_ignore_columns(cfg, dim),
    )


def _define_silver_stream(name: str, spec: dict) -> None:
    """Streaming table: Kafka events (stream) left-joined to the current dimensions (static)."""
    @dp.table(name=table_fqn(cfg, "silver", name), comment=f"{name} events enriched with dimensions (silver)")
    @with_expectations(EXPECTATIONS, name)
    def _stream():
        stream = spark.readStream.table(NAMES[f"bronze_{spec['bronze']}"]).selectExpr(*spec["exprs"])
        if spec.get("dedupe_on"):
            stream = stream.dropDuplicates(spec["dedupe_on"])
        for static in spec["static"]:
            stream = stream.join(spark.sql(T.render(static["sql"], cfg)), on=static["on"], how="left")
        return stream


def _define_silver_view(name: str, template: str) -> None:
    @dp.materialized_view(name=table_fqn(cfg, "silver", name), comment=f"Cleansed {name} (silver)")
    @with_expectations(EXPECTATIONS, name)
    def _dataset():
        return spark.sql(T.render(template, cfg))


for _dim, _spec in T.SCD2_SOURCES.items():
    _define_scd2_dimension(_dim, _spec)

for _name, _spec in T.SILVER_STREAMS.items():
    _define_silver_stream(_name, _spec)

for _name, _template in T.SILVER_SQL.items():
    _define_silver_view(_name, _template)
