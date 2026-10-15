"""Configuration model and loader for the MongoDB CDC pipeline.

The pipeline is metadata driven: every source collection is described
by an entry in a YAML (or JSON) file, and the pipeline builds one
bronze table, one silver feed view, and one merged silver table per
entry — plus optional typed projection views and gold views. The path
of the metadata file and the SDLC environment name are supplied through
the pipeline ``configuration`` block (``cdc.config_path`` /
``cdc.environment``), so the same code runs unchanged in dev, test,
and prod.

Because MongoDB is schemaless and the document is stored as VARIANT,
a collection needs far less metadata than a relational table: no keys
(always ``_id``), no schema hints, no column transforms — typed access
is declared as projection expressions instead.

Everything in this module is plain Python (no Spark imports) so it can
be unit tested anywhere, including CI runners without a Spark runtime.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

EXPECTATION_ACTIONS = ("warn", "drop", "fail")
VALID_SCD_TYPES = (1, 2)
DEFAULT_TOPIC_TEMPLATE = "mongodb.{database}.{collection}"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    """Raised when the pipeline metadata file is invalid.

    The message aggregates every problem found so a bad file can be
    fixed in one pass instead of one failed pipeline update at a time.
    """


@dataclass(frozen=True)
class CollectionConfig:
    """Everything the pipeline needs to know about one collection.

    Attributes:
        name: Logical collection name; used to derive dataset names.
        source_database: Source MongoDB database (topic segment).
        path: Fully resolved landing directory for this collection.
        scd_type: 1 (current state) or 2 (full history) for silver.
        comment: Optional human description propagated to the catalog.
        cluster_by: Liquid clustering columns for the silver table.
        projection: SQL expressions over the ``doc`` VARIANT column; if
            non-empty, a "<name>_typed" materialized view is created.
        expectations: Data-quality rules per action ("warn", "drop",
            "fail"), each a mapping of rule name -> SQL constraint,
            evaluated against the silver feed (``_id``, ``doc``,
            ``_cdc_*`` columns).
    """

    name: str
    source_database: str
    path: str
    scd_type: int = 1
    comment: str = ""
    cluster_by: tuple[str, ...] = ()
    projection: tuple[str, ...] = ()
    expectations: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def bronze_name(self) -> str:
        """Name of the raw-events streaming table."""
        return f"{self.name}_bronze"

    @property
    def silver_feed_name(self) -> str:
        """Name of the parsed view feeding the AUTO CDC flow."""
        return f"{self.name}_silver_feed"

    @property
    def silver_name(self) -> str:
        """Name of the merged silver streaming table."""
        return f"{self.name}_silver"

    @property
    def typed_name(self) -> str:
        """Name of the optional typed projection view."""
        return f"{self.name}_typed"


@dataclass(frozen=True)
class GoldViewConfig:
    """A gold-layer materialized view defined entirely in metadata."""

    name: str
    sql: str
    comment: str = ""


@dataclass(frozen=True)
class PipelineConfig:
    """The fully validated pipeline metadata for one environment."""

    environment: str
    landing_root: str
    collections: tuple[CollectionConfig, ...]
    gold_views: tuple[GoldViewConfig, ...] = ()


def load_config(
    path: str | Path,
    environment: str,
    landing_root: str | None = None,
) -> PipelineConfig:
    """Load and validate the metadata file for the given environment.

    ``{env}`` placeholders in ``landing_root`` (and per-collection
    ``path`` overrides) are replaced with ``environment``, which is how
    one metadata file serves dev, test, and prod.

    ``landing_root``, when given, replaces the file's ``landing_root``
    (the ``cdc.landing_root`` pipeline setting). This lets a deployment
    whose volumes live elsewhere, e.g. the ``workspace`` catalog on
    Free Edition, run the committed metadata unchanged.

    Raises:
        ConfigError: if the file is missing, unparsable, or invalid.
    """
    raw = _parse_file(Path(path))
    errors: list[str] = []

    if landing_root is None:
        landing_root = _string(raw, "landing_root", errors)
    landing_root = landing_root.replace("{env}", environment).rstrip("/")
    topic_template = raw.get("topic_template", DEFAULT_TOPIC_TEMPLATE)
    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, dict):
        errors.append("'defaults' must be a mapping.")
        defaults = {}

    collections = _build_collections(
        raw.get("collections"), defaults, landing_root, topic_template,
        environment, errors,
    )
    gold_views = _build_gold_views(raw.get("gold"), errors)

    _check_unique_names(collections, gold_views, errors)
    if errors:
        raise ConfigError(
            "Invalid pipeline metadata in {}:\n- {}".format(
                path, "\n- ".join(errors)
            )
        )
    return PipelineConfig(
        environment=environment,
        landing_root=landing_root,
        collections=collections,
        gold_views=gold_views,
    )


def _parse_file(path: Path) -> dict[str, Any]:
    """Parse a YAML or JSON metadata file into a dictionary."""
    if not path.is_file():
        raise ConfigError(f"Pipeline metadata file not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        if path.suffix.lower() in (".yaml", ".yml"):
            import yaml  # deferred: only needed for YAML files

            parsed = yaml.safe_load(text)
        else:
            parsed = json.loads(text)
    except Exception as exc:  # noqa: BLE001 - rewrap with file context
        raise ConfigError(f"Could not parse {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"{path} must contain a mapping at the top level.")
    return parsed


def _build_collections(
    entries: Any,
    defaults: dict[str, Any],
    landing_root: str,
    topic_template: str,
    environment: str,
    errors: list[str],
) -> tuple[CollectionConfig, ...]:
    if not isinstance(entries, list) or not entries:
        errors.append("'collections' must be a non-empty list.")
        return ()
    collections = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            errors.append(f"collections[{index}] must be a mapping.")
            continue
        merged = {**defaults, **entry}
        collection = _build_collection(
            merged, index, landing_root, topic_template, environment, errors
        )
        if collection is not None:
            collections.append(collection)
    return tuple(collections)


def _build_collection(
    entry: dict[str, Any],
    index: int,
    landing_root: str,
    topic_template: str,
    environment: str,
    errors: list[str],
) -> CollectionConfig | None:
    label = f"collections[{index}]"
    name = entry.get("name", "")
    if not _IDENTIFIER.match(str(name)):
        errors.append(f"{label}: 'name' must be a valid identifier.")
        return None
    label = f"collection '{name}'"

    database = str(entry.get("source_database", ""))
    if not database:
        errors.append(f"{label}: 'source_database' is required.")

    scd_type = entry.get("scd_type", 1)
    if scd_type not in VALID_SCD_TYPES:
        errors.append(f"{label}: 'scd_type' must be one of {VALID_SCD_TYPES}.")

    expectations = _expectations(entry.get("expectations"), label, errors)

    projection = _string_tuple(entry.get("projection"), split_commas=False)

    path = str(
        entry.get("path")
        or "{}/{}".format(
            landing_root,
            topic_template.format(database=database, collection=name),
        )
    ).replace("{env}", environment)

    return CollectionConfig(
        name=str(name),
        source_database=database,
        path=path,
        scd_type=int(scd_type) if scd_type in VALID_SCD_TYPES else 1,
        comment=str(entry.get("comment", "")),
        cluster_by=_string_tuple(entry.get("cluster_by")),
        projection=projection,
        expectations=expectations,
    )


def _build_gold_views(
    entries: Any, errors: list[str]
) -> tuple[GoldViewConfig, ...]:
    if entries is None:
        return ()
    if not isinstance(entries, list):
        errors.append("'gold' must be a list.")
        return ()
    views = []
    for index, entry in enumerate(entries):
        label = f"gold[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{label} must be a mapping.")
            continue
        name = entry.get("name", "")
        sql = str(entry.get("sql", "")).strip()
        if not _IDENTIFIER.match(str(name)):
            errors.append(f"{label}: 'name' must be a valid identifier.")
            continue
        if not sql:
            errors.append(f"gold view '{name}': 'sql' is required.")
            continue
        views.append(
            GoldViewConfig(
                name=str(name), sql=sql, comment=str(entry.get("comment", ""))
            )
        )
    return tuple(views)


def _expectations(
    value: Any, label: str, errors: list[str]
) -> dict[str, dict[str, str]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label}: 'expectations' must be a mapping.")
        return {}
    result: dict[str, dict[str, str]] = {}
    for action, rules in value.items():
        if action not in EXPECTATION_ACTIONS:
            errors.append(
                f"{label}: unknown expectation action '{action}' "
                f"(expected one of {EXPECTATION_ACTIONS})."
            )
            continue
        if not isinstance(rules, dict):
            errors.append(f"{label}: expectations.{action} must be a mapping.")
            continue
        result[action] = {str(k): str(v) for k, v in rules.items()}
    return result


def _check_unique_names(
    collections: tuple[CollectionConfig, ...],
    gold_views: tuple[GoldViewConfig, ...],
    errors: list[str],
) -> None:
    seen: set[str] = set()
    names = [c.name for c in collections] + [g.name for g in gold_views]
    for name in names:
        if name in seen:
            errors.append(f"Duplicate dataset name '{name}'.")
        seen.add(name)


def _string(raw: dict[str, Any], key: str, errors: list[str]) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"'{key}' is required and must be a non-empty string.")
        return ""
    return value.strip()


def _string_tuple(value: Any, split_commas: bool = True) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = value.split(",") if split_commas else [value]
    return tuple(str(v).strip() for v in value if str(v).strip())
