"""Metadata-driven Debezium MongoDB CDC framework for Lakeflow.

Public surface:
    - :mod:`cdc_framework.config` — metadata model and loader
      (Spark-free).
    - :mod:`cdc_framework.debezium` — envelope constants and helpers
      (Spark-free).
    - :mod:`cdc_framework.flows` — dataset factories (requires a
      pipeline runtime; import it only inside a pipeline update).
"""

from cdc_framework.config import (
    CollectionConfig,
    ConfigError,
    GoldViewConfig,
    PipelineConfig,
    load_config,
)

__all__ = [
    "CollectionConfig",
    "ConfigError",
    "GoldViewConfig",
    "PipelineConfig",
    "load_config",
]
