"""Entry point for the metadata-driven MongoDB CDC pipeline.

This is the single source file registered with the Lakeflow pipeline.
It reads three settings from the pipeline ``configuration`` block:

    cdc.environment  SDLC environment name (dev | test | prod); used to
                     resolve ``{env}`` placeholders in landing paths.
    cdc.config_path  Path to the collection metadata file (YAML or
                     JSON) deployed by the bundle.
    cdc.src_path     Deployed src/ directory, used to import the
                     cdc_framework package.

It then registers bronze/silver datasets for every collection in the
metadata, plus typed projections and gold views. Adding a new source
collection is a metadata change only — no code changes required.
"""

import sys

from pyspark.sql import SparkSession


def _active_spark() -> SparkSession:
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession: cdc_pipeline.py must run inside a "
            "Lakeflow pipeline."
        )
    return session


spark = _active_spark()


def _required_setting(key: str) -> str:
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(
            f"Pipeline configuration '{key}' must be set in the pipeline "
            "settings (see resources/cdc_pipeline.pipeline.yml)."
        )
    return value


# The runtime executes this file as a notebook-style cell (__file__ is
# undefined), so the deployed src/ directory is passed in explicitly to
# make the cdc_framework package importable.
sys.path.insert(0, _required_setting("cdc.src_path"))

from cdc_framework import flows, load_config  # noqa: E402

config = load_config(
    path=_required_setting("cdc.config_path"),
    environment=_required_setting("cdc.environment"),
    # Optional: overrides the metadata file's landing_root (bundle variable
    # `landing_root`); empty/unset means "use the value from the file".
    landing_root=spark.conf.get("cdc.landing_root", None) or None,
)

for collection in config.collections:
    flows.register_collection(spark, collection)
for view in config.gold_views:
    flows.register_gold_view(spark, view)
