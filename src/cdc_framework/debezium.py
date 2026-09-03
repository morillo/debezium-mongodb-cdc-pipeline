"""Debezium MongoDB constants and helpers.

Kept free of Spark imports so it is unit testable without a runtime.

CDC bookkeeping columns carried through the silver feed use a ``_cdc_``
prefix so they can never collide with anything, and they are stripped
from the silver table by the AUTO CDC ``except_column_list``.
"""

from __future__ import annotations

CDC_OP = "_cdc_op"
CDC_TS_MS = "_cdc_ts_ms"
CDC_ORD = "_cdc_ord"
CDC_SOURCE_DB = "_cdc_source_db"
CDC_SOURCE_COLLECTION = "_cdc_source_collection"

#: All bookkeeping columns, i.e. the AUTO CDC ``except_column_list``.
CDC_METADATA_COLUMNS = (
    CDC_OP,
    CDC_TS_MS,
    CDC_ORD,
    CDC_SOURCE_DB,
    CDC_SOURCE_COLLECTION,
)

#: Debezium delete marker, evaluated against the silver feed.
DELETE_PREDICATE = f"{CDC_OP} = 'd'"

#: Kafka tombstones / malformed events carry no ``op``; drop them.
TOMBSTONE_EXPECTATION = ("valid_op", f"{CDC_OP} IS NOT NULL")

#: Events whose document id cannot be resolved cannot be merged; this
#: also catches deletes emitted without pre-images (no document at all).
ID_EXPECTATION = ("valid_document_id", "_id IS NOT NULL")

#: Deletes carry the document in ``before`` (pre-images); everything
#: else carries the full document in ``after``.
DOC_EXPR = "parse_json(CASE WHEN op = 'd' THEN before ELSE after END)"

#: MongoDB's _id in extended JSON is usually {"$oid": "..."} but may be
#: any scalar or even a composite document. The merge key must be a
#: stable string, so try each representation in turn.
DOC_ID_EXPR = (
    "coalesce("
    "try_variant_get(doc, '$._id[\"$oid\"]', 'string'), "
    "try_variant_get(doc, '$._id', 'string'), "
    "to_json(try_variant_get(doc, '$._id'))"
    ")"
)
