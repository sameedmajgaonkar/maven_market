"""MongoDB Atlas as a Spark Python *streaming* data source (snapshot-on-change), for the Bronze layer.

Why a custom source: a Lakeflow streaming table needs a streaming query, and a plain PyMongo read is a
batch DataFrame (only usable in a one-time flow that re-runs on a full refresh, so it would never see an
Atlas edit). The Python Data Source API gives the pipeline a real micro-batch source with a checkpointed
offset instead (see ``maven_market.ingestion.mongo_ingest`` for the semantics: snapshot-on-change, not CDC).

Pipeline usage (see ``maven_market/pipelines/bronze.py``)::

    spark.dataSource.register(mongo_source.build())
    spark.readStream.format(mongo_source.SOURCE_NAME).options(...).load()

``build()`` defines the classes inside a function on purpose: cloudpickle serialises locally defined
classes *by value*, so the Python worker that runs the reader does not have to import ``maven_market``
(only the pipeline driver has the repo's ``src`` folder on its path). The reader adds ``src_path`` itself
before importing the pure helpers, and imports ``pymongo`` lazily (a pipeline environment dependency).
"""
from __future__ import annotations

SOURCE_NAME = "maven_mongo"


def build():  # pragma: no cover - needs a Spark runtime with the Python Data Source API
    """Return the ``DataSource`` class to pass to ``spark.dataSource.register``."""
    from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader

    def _helpers(src_path):
        import sys

        if src_path and src_path not in sys.path:
            sys.path.append(src_path)
        from maven_market.ingestion import mongo_ingest

        return mongo_ingest

    class MavenMongoReader(SimpleDataSourceStreamReader):
        def __init__(self, schema, options):
            self._schema = schema
            self._options = dict(options)

        def initialOffset(self):
            return {"digest": ""}

        def _snapshot(self, committed_digest):
            m = _helpers(self._options.get("src_path"))
            ddl = self._options["ddl"]
            columns = m.schema_columns(ddl)
            docs = m.fetch_documents(self._options["uri"], self._options["database"], self._options["collection"])
            rows = m.documents_to_rows(docs, columns, m.schema_types(ddl))
            emit, digest = m.plan_snapshot(rows, columns, self._options["id_field"], committed_digest)
            return [tuple(r[c] for c in columns) for r in emit], digest

        def read(self, start):
            rows, digest = self._snapshot(start.get("digest"))
            if not rows:
                return iter([]), start
            return iter(rows), {"digest": digest}

        def readBetweenOffsets(self, start, end):
            # Replay of an uncommitted batch (after a failure). Mongo keeps no history, so this re-reads the
            # current snapshot; Silver's SCD2 flow ignores re-delivered, unchanged rows.
            rows, _ = self._snapshot(None)
            return iter(rows)

        def commit(self, end):
            pass

    class MavenMongoDataSource(DataSource):
        @classmethod
        def name(cls):
            return SOURCE_NAME

        def schema(self):
            return self.options["ddl"]

        def simpleStreamReader(self, schema):
            return MavenMongoReader(schema, self.options)

    return MavenMongoDataSource
