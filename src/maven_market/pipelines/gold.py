"""Gold layer: star schema (conformed dimensions + facts) and BI aggregates.

Reads Silver/Gold datasets of the same pipeline; Lakeflow derives the dependency order from the
table references in each query. Definitions live in ``maven_market.pipelines.transforms``.
"""
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

_src_path = spark.conf.get("maven.src_path")
if _src_path not in sys.path:
    sys.path.append(_src_path)

from maven_market.config import load_config, table_fqn  # noqa: E402
from maven_market.pipelines import transforms as T  # noqa: E402

cfg = load_config(spark.conf.get("maven.env"), config_dir=spark.conf.get("maven.config_dir"))


def _define_gold_view(name: str, template: str) -> None:
    @dp.materialized_view(name=table_fqn(cfg, "gold", name), comment=f"{name} (gold)")
    def _dataset():
        return spark.sql(T.render(template, cfg))


for _name, _template in T.GOLD_SQL.items():
    _define_gold_view(_name, _template)
