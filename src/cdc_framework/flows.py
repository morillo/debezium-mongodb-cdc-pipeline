"""Dataset factories for the metadata-driven MongoDB CDC pipeline.

Uses ``pyspark.pipelines`` — the current Lakeflow Declarative Pipelines
Python API. Every factory takes a
:class:`~cdc_framework.config.CollectionConfig` and registers the
datasets for that one collection, so the pipeline scales to any number
of source collections by looping over the metadata.

Per collection the medallion layout is::

    <name>_bronze       raw Debezium events (Auto Loader, streaming)
    <name>_silver_feed  parsed, quality-gated CDC feed (temp view)
    <name>_silver       merged state: _id + doc VARIANT (AUTO CDC flow)
    <name>_typed        optional typed projection (materialized view)

plus any number of metadata-defined gold materialized views.

There is no schema handling anywhere: the Debezium MongoDB envelope is
identical for every collection (the document travels as a string), and
the document itself is stored as VARIANT, so schema drift needs no
inference, no registry, and no pipeline changes.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from .config import CollectionConfig, GoldViewConfig
from .debezium import (
    CDC_METADATA_COLUMNS,
    CDC_OP,
    CDC_ORD,
    CDC_SOURCE_COLLECTION,
    CDC_SOURCE_DB,
    CDC_TS_MS,
    DELETE_PREDICATE,
    DOC_EXPR,
    DOC_ID_EXPR,
    ID_EXPECTATION,
    TOMBSTONE_EXPECTATION,
)

# The Debezium MongoDB envelope is identical for every collection: the
# document travels as a string, so unlike relational connectors there
# is no per-table schema to derive. `ord` is the oplog ordinal within a
# cluster-time second — the tiebreaker for same-millisecond events.
SOURCE_SCHEMA = StructType(
    [
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("rs", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("ord", LongType(), True),
    ]
)

ENVELOPE = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField("before", StringType(), True),
                    StructField("after", StringType(), True),
                    StructField("source", SOURCE_SCHEMA, True),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("schema", StringType(), True),
    ]
)


def register_collection(
    spark: SparkSession, collection: CollectionConfig
) -> None:
    """Register the bronze/silver datasets for one collection."""
    _register_bronze(spark, collection)
    _register_silver_feed(spark, collection)
    _register_silver(collection)
    if collection.projection:
        _register_typed(spark, collection)


def register_gold_view(spark: SparkSession, view: GoldViewConfig) -> None:
    """Register one metadata-defined gold materialized view."""

    def gold() -> DataFrame:
        return spark.sql(view.sql)

    dp.materialized_view(
        name=view.name,
        comment=view.comment or "Gold view defined in pipeline metadata.",
        table_properties={"quality": "gold"},
    )(gold)


def _register_bronze(
    spark: SparkSession, collection: CollectionConfig
) -> None:
    """Ingest raw Debezium JSON envelopes with Auto Loader."""

    def bronze() -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(ENVELOPE)
            .load(collection.path)
        )

    dp.table(
        name=collection.bronze_name,
        comment=(
            f"Raw Debezium CDC events for "
            f"{collection.source_database}.{collection.name}, ingested "
            f"with Auto Loader from {collection.path}"
        ),
        table_properties={"quality": "bronze"},
    )(bronze)


def _register_silver_feed(
    spark: SparkSession, collection: CollectionConfig
) -> None:
    """Parse the envelope into a quality-gated CDC feed.

    The document string becomes a VARIANT column, ``_id`` is extracted
    as a stable string merge key, and metadata-defined expectations are
    applied here, before the merge.

    SCD type 2 collections keep the document as canonical JSON text
    (``doc_json``) instead of VARIANT: tracking history requires the
    engine to compare consecutive row versions, and VARIANT does not
    support equality comparison. The typed projection view parses it
    back to VARIANT, so consumers see the same ``doc`` either way.
    """

    def silver_feed() -> DataFrame:
        events = spark.readStream.table(collection.bronze_name).select(
            "payload.*"
        )
        parsed = events.select(
            F.col("op").alias(CDC_OP),
            F.col("ts_ms").alias(CDC_TS_MS),
            F.col("source.ord").alias(CDC_ORD),
            F.col("source.db").alias(CDC_SOURCE_DB),
            F.col("source.collection").alias(CDC_SOURCE_COLLECTION),
            F.expr(DOC_EXPR).alias("doc"),
        )
        parsed = parsed.withColumn("_id", F.expr(DOC_ID_EXPR))
        if collection.scd_type == 2:
            parsed = parsed.withColumn(
                "doc_json", F.expr("to_json(doc)")
            ).drop("doc")
        return parsed

    # Databricks adds expectations to pyspark.pipelines; OSS stubs lack them.
    silver_feed = dp.expect_or_drop(*TOMBSTONE_EXPECTATION)(  # type: ignore
        silver_feed
    )
    silver_feed = dp.expect_or_drop(*ID_EXPECTATION)(  # type: ignore
        silver_feed
    )
    if collection.expectations.get("drop"):
        silver_feed = dp.expect_all_or_drop(  # type: ignore
            collection.expectations["drop"]
        )(silver_feed)
    if collection.expectations.get("warn"):
        silver_feed = dp.expect_all(  # type: ignore
            collection.expectations["warn"]
        )(silver_feed)
    if collection.expectations.get("fail"):
        silver_feed = dp.expect_all_or_fail(  # type: ignore
            collection.expectations["fail"]
        )(silver_feed)
    dp.temporary_view(
        name=collection.silver_feed_name,
        comment=f"Parsed CDC feed for {collection.name} "
        "(drives AUTO CDC).",
    )(silver_feed)


def _register_silver(collection: CollectionConfig) -> None:
    """Merge the CDC feed into the silver table with an AUTO CDC flow.

    ``ts_ms`` alone has millisecond granularity and can tie for rapid
    changes to the same document, so the oplog ordinal breaks ties
    deterministically.
    """
    dp.create_streaming_table(
        name=collection.silver_name,
        comment=collection.comment
        or (
            f"State of {collection.source_database}.{collection.name}: "
            f"one row per _id, document stored as VARIANT (SCD type "
            f"{collection.scd_type}, deletes applied)."
        ),
        table_properties={"quality": "silver"},
        cluster_by=list(collection.cluster_by) or None,
    )
    dp.create_auto_cdc_flow(
        target=collection.silver_name,
        source=collection.silver_feed_name,
        keys=["_id"],
        sequence_by=F.struct(F.col(CDC_TS_MS), F.col(CDC_ORD)),
        apply_as_deletes=F.expr(DELETE_PREDICATE),
        except_column_list=list(CDC_METADATA_COLUMNS),
        # Databricks supports SCD type 2 here; OSS stubs only declare type 1.
        stored_as_scd_type=collection.scd_type,  # type: ignore
    )


def _register_typed(
    spark: SparkSession, collection: CollectionConfig
) -> None:
    """Materialize the metadata-defined typed projection.

    For SCD type 2 silver tables only current rows are projected, so
    the typed view always represents the present state.
    """

    def typed() -> DataFrame:
        df = spark.read.table(collection.silver_name)
        if collection.scd_type == 2:
            df = df.where(F.col("__END_AT").isNull()).withColumn(
                "doc", F.expr("parse_json(doc_json)")
            )
        return df.selectExpr("_id", *collection.projection)

    dp.materialized_view(
        name=collection.typed_name,
        comment=f"Typed projection of {collection.silver_name}, defined "
        "in pipeline metadata.",
        table_properties={"quality": "gold"},
    )(typed)
