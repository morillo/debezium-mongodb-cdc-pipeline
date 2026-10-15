"""Unit tests for the metadata model and loader (no Spark required)."""

import pytest

from cdc_framework.config import ConfigError, load_config

VALID_YAML = """
landing_root: /Volumes/cdc_{env}/landing/debezium
topic_template: mongodb.{database}.{collection}

defaults:
  source_database: shopdb

collections:
  - name: customers
    scd_type: 2
    cluster_by: [_id]
    projection:
      - "doc:name::string AS name"
      - "doc:address.city::string AS city"
    expectations:
      drop:
        valid_email: "doc:email IS NULL OR doc:email::string LIKE '%@%'"
  - name: orders
    path: /Volumes/other_{env}/landing/special/orders

gold:
  - name: gold_counts
    comment: Counts.
    sql: SELECT 1 AS one
"""


def write(tmp_path, text, suffix=".yaml"):
    path = tmp_path / f"collections{suffix}"
    path.write_text(text, encoding="utf-8")
    return path


def test_landing_root_override_replaces_file_value(tmp_path):
    config = load_config(
        write(tmp_path, VALID_YAML),
        environment="dev",
        landing_root="/Volumes/workspace/landing/debezium/",
    )

    assert config.landing_root == "/Volumes/workspace/landing/debezium"
    customers, orders = config.collections
    assert customers.path == (
        "/Volumes/workspace/landing/debezium/mongodb.shopdb.customers"
    )
    # per-collection path overrides are untouched by the landing_root override
    assert orders.path == "/Volumes/other_dev/landing/special/orders"


def test_landing_root_override_still_resolves_env(tmp_path):
    config = load_config(
        write(tmp_path, VALID_YAML),
        environment="test",
        landing_root="/mnt/{env}/debezium",
    )
    assert config.landing_root == "/mnt/test/debezium"


def test_loads_valid_yaml_and_resolves_environment(tmp_path):
    config = load_config(write(tmp_path, VALID_YAML), environment="test")

    assert config.environment == "test"
    assert config.landing_root == "/Volumes/cdc_test/landing/debezium"

    customers, orders = config.collections
    assert customers.name == "customers"
    assert customers.source_database == "shopdb"  # from defaults
    assert customers.scd_type == 2
    assert customers.path == (
        "/Volumes/cdc_test/landing/debezium/mongodb.shopdb.customers"
    )
    assert customers.bronze_name == "customers_bronze"
    assert customers.silver_feed_name == "customers_silver_feed"
    assert customers.silver_name == "customers_silver"
    assert customers.typed_name == "customers_typed"
    assert customers.projection == (
        "doc:name::string AS name",
        "doc:address.city::string AS city",
    )
    assert "valid_email" in customers.expectations["drop"]

    # Per-collection overrides win over derived paths.
    assert orders.path == "/Volumes/other_test/landing/special/orders"
    assert orders.scd_type == 1
    assert orders.projection == ()

    (gold,) = config.gold_views
    assert gold.name == "gold_counts"
    assert gold.sql == "SELECT 1 AS one"


def test_loads_json_config(tmp_path):
    text = (
        '{"landing_root": "/mnt/landing", "collections": '
        '[{"name": "c1", "source_database": "db"}]}'
    )
    config = load_config(write(tmp_path, text, ".json"), environment="dev")
    assert config.collections[0].path == "/mnt/landing/mongodb.db.c1"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "absent.yaml", environment="dev")


def test_invalid_yaml_raises(tmp_path):
    with pytest.raises(ConfigError, match="Could not parse"):
        load_config(write(tmp_path, "a: [unclosed"), environment="dev")


def test_all_errors_reported_at_once(tmp_path):
    text = """
collections:
  - name: "bad name!"
  - name: c1
    scd_type: 3
    expectations:
      explode: {r: "1 = 1"}
  - name: c1
    source_database: db
gold:
  - name: g1
"""
    with pytest.raises(ConfigError) as excinfo:
        load_config(write(tmp_path, text), environment="dev")
    message = str(excinfo.value)
    assert "'landing_root' is required" in message
    assert "'name' must be a valid identifier" in message
    assert "'source_database' is required" in message
    assert "'scd_type' must be one of" in message
    assert "unknown expectation action 'explode'" in message
    assert "Duplicate dataset name 'c1'" in message
    assert "gold view 'g1': 'sql' is required" in message


def test_empty_collections_rejected(tmp_path):
    with pytest.raises(ConfigError, match="non-empty list"):
        load_config(
            write(tmp_path, "landing_root: /mnt/x\ncollections: []"),
            environment="dev",
        )


def test_projection_string_not_split_on_commas(tmp_path):
    text = """
landing_root: /mnt/landing
collections:
  - name: c1
    source_database: db
    projection: "concat(doc:a::string, doc:b::string) AS ab"
"""
    config = load_config(write(tmp_path, text), environment="dev")
    assert config.collections[0].projection == (
        "concat(doc:a::string, doc:b::string) AS ab",
    )
