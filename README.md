# debezium-mongodb-cdc-pipeline — metadata-driven schemaless CDC

A metadata-driven framework that turns Debezium (MongoDB)
change-data-capture events into up-to-date and historical tables in a
Unity Catalog medallion architecture, built on **Lakeflow Declarative
Pipelines** (formerly Delta Live Tables) with the current
`pyspark.pipelines` Python API and the **VARIANT** data type — no
schema inference, no schema registry, no reload on schema drift.

Adding a source collection — whether it's the 10th or the 100th — is a
one-entry change in `config/collections.yaml`. No code changes.

## Results

The pipeline was deployed and run end to end on Databricks Free
Edition (serverless compute, Unity Catalog `workspace` catalog) against
hand-crafted Debezium MongoDB change events for three collections.
SCD 1 and SCD 2 merges, pre-image deletes, same-millisecond event
ordering by oplog `ord`, tombstone dropping, schema-drift documents,
typed projections and a gold view joining collections on a VARIANT
extraction all behaved as designed; see
[Verifying the results](#verifying-the-results) for the queries and
their output.

![Completed pipeline run on Databricks Free Edition: three collections flow from landing volume to bronze, silver feed and silver, then into typed projections and the gold view](docs/images/pipeline-graph.png)

Limitation: this is a functional verification, not a benchmark. There
are no throughput or latency numbers, the sample events are not
committed, and CI runs unit tests only for the Spark-free modules, so
changes to `flows.py` are verified only by deploying to a dev target.

## About this project

This repository is a **reference architecture**: it exists to
demonstrate, end to end, how to build metadata-driven CDC ingestion on
Databricks following data engineering, software engineering, and CI/CD
best practices — configuration as data, a validating loader that fails
fast, Spark-free unit tests, an Asset Bundle with dev/test/prod
targets, and a gated deployment workflow. It is not operating a
production workload.

Companion projects:
[debezium-cdc-pipeline](https://github.com/morillo/debezium-cdc-pipeline)
(the relational/MySQL twin of this framework),
[dlt-debezium-mysql](https://github.com/morillo/dlt-debezium-mysql) and
[dlt-debezium-mongodb](https://github.com/morillo/dlt-debezium-mongodb)
(single-source pipelines).

## Architecture

```mermaid
flowchart LR
    A[Debezium JSON events\nlanding zone] -->|Auto Loader\nstatic envelope| B["&lt;coll&gt;_bronze\nraw events"]
    B --> C["&lt;coll&gt;_silver_feed\nparse + quality gates"]
    C -->|AUTO CDC flow\nSCD 1 or 2| D["&lt;coll&gt;_silver\n_id + document"]
    D --> E["&lt;coll&gt;_typed\nprojection views"]
    D --> F[gold materialized views\ndefined in metadata]
```

Per collection in the metadata, the pipeline registers:

| Dataset | Kind | Purpose |
|---|---|---|
| `<name>_bronze` | streaming table | Raw Debezium envelopes via Auto Loader (static schema). |
| `<name>_silver_feed` | temporary view | Document parsed, `_id` extracted (ObjectId / scalar / composite), `_cdc_*` bookkeeping, expectations applied. |
| `<name>_silver` | streaming table | AUTO CDC merge keyed on `_id`, sequenced by `(ts_ms, oplog ord)`, deletes applied. |
| `<name>_typed` | materialized view | Optional typed projection from metadata (current rows only for SCD2). |

Gold materialized views are declared entirely in metadata (`name` +
`sql`) and can join collections on VARIANT extractions.

## Project layout

```
debezium-mongodb-cdc-pipeline/
├── databricks.yml                     # Asset Bundle: dev / test / prod targets
├── resources/cdc_pipeline.pipeline.yml# Pipeline resource (UC catalog/schema)
├── config/collections.yaml            # Collection + gold metadata ({env}-templated)
├── src/
│   ├── cdc_pipeline.py                # Pipeline entry point
│   └── cdc_framework/
│       ├── config.py                  # Metadata model + validating loader
│       ├── debezium.py                # Envelope constants/helpers (Spark-free)
│       └── flows.py                   # Dataset factories (pyspark.pipelines)
├── tests/                             # Unit tests (run anywhere, no Spark)
├── docs/images/                       # README screenshots
├── .github/workflows/ci.yml           # Secret scan, lint, test, deploy
├── .github/dependabot.yml             # Weekly uv + Actions updates
└── .pre-commit-config.yaml            # gitleaks hook
```

## How a collection is configured

```yaml
collections:
  - name: customers
    source_database: shopdb   # or inherit from `defaults:`
    scd_type: 2               # keep full history
    cluster_by: [_id]         # liquid clustering on silver
    projection:
      - "doc:name::string AS name"
      - "doc:address.city::string AS city"
      - "cast(doc:signup['$date']::long / 1000 as timestamp) AS signup_at"
    expectations:
      drop:
        valid_email: >-
          doc_json:email IS NULL OR doc_json:email::string LIKE '%@%'
```

The landing path defaults to `{landing_root}/{topic_template}` (e.g.
`/Volumes/cdc_dev/landing/debezium/mongodb.shopdb.customers`) and can
be overridden per collection with `path:`. `{env}` placeholders resolve
to the `cdc.environment` pipeline setting. The merge key is always the
document `_id`, extracted from extended JSON whatever its shape:
`{"$oid": ...}`, plain scalars, or composite keys (kept as canonical
JSON text).

Built-in safeguards applied to every collection automatically:

- **Tombstones / malformed events** (`op IS NULL`) are dropped and
  counted in the pipeline event log.
- **Events without a resolvable `_id`** (including deletes emitted
  without pre-images) are dropped and counted.
- **Deterministic ordering**: events are sequenced by
  `struct(ts_ms, ord)` so same-millisecond changes to a document
  resolve by oplog ordinal.

## Schema handling

### Why MongoDB CDC needs no schema machinery here

Debezium's MongoDB connector emits each document as a single JSON
string with no schema metadata — MongoDB is schemaless, so there is
nothing to describe. Historically that gap was closed with custom
schema inference (`schema_of_json()`/`from_json()` plus a stored
schema registry). This framework closes it with types instead:

- the **envelope is static and identical for every collection** (the
  document travels as a string), so there is no per-collection schema
  to derive — the exact opposite of the relational twin;
- the **document is stored as VARIANT** (`parse_json`), so drift is a
  non-event and consumers project types at read time;
- typed access is **declared in metadata** as projection expressions,
  materialized per collection as a `<name>_typed` view.

### VARIANT and SCD types — an engine constraint worth knowing

Discovered and verified on a live workspace:

- **SCD type 1** (current state): the document is stored as **VARIANT**
  — merge works, drift is free, this is the default and the fast path.
- **SCD type 2** (full history): the engine tracks history by comparing
  consecutive row versions, and VARIANT does not support equality
  comparison (`INVALID_ORDERING_TYPE`). For SCD2 collections the
  framework therefore stores the document as **canonical JSON text**
  (`doc_json`) — deterministic, so cosmetic key-order differences
  cannot create phantom history rows — and the typed projection view
  parses it back to a `doc` VARIANT, so **projection metadata is
  identical for both SCD types**.

The only place the difference shows is SQL you write yourself against
an SCD2 silver table: use `doc_json:path` instead of `doc:path` (the
`:` operator works on JSON strings natively).

## SDLC: dev → test → prod

The bundle defines three targets; each pins a Unity Catalog catalog
(`cdc_dev` / `cdc_test` / `cdc_prod`) and an environment name that
resolves the landing paths. Code and metadata are identical across
environments — only configuration differs.

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev     # per-user dev copy (mode: development)
databricks bundle run      -t dev debezium_mongodb_cdc

databricks bundle deploy   -t test    # via CI, as a service principal
databricks bundle deploy   -t prod    # via CI, gated release
```

The landing root defaults to `landing_root` in `config/collections.yaml`
(`/Volumes/cdc_<env>/landing/debezium`). Workspaces whose volumes live
elsewhere override it per deployment without editing the metadata file,
e.g. Databricks Free Edition (only the `workspace` catalog exists):

```bash
databricks bundle deploy -t dev --var catalog=workspace \
  --var landing_root=/Volumes/workspace/landing/debezium
```

No target commits a `workspace.host`. Set it (and the `run_as`
service principals for test/prod) in `databricks.yml` before first use,
or keep your dev host out of git in `resources/workspace.local.yml`
(git-ignored, picked up by the bundle's `include: resources/*.yml`):

```yaml
targets:
  dev:
    workspace:
      host: https://<your-dev-workspace>.cloud.databricks.com
```

The Databricks VS Code extension needs this host to offer sign-in. The
same file can also hold per-workspace variable values; see
[Running from VS Code](#running-from-vs-code).

## CI/CD

`.github/workflows/ci.yml` implements the standard flow:

1. Every PR: `gitleaks` secret scan (full history), `ruff` lint,
   `pyright` type check, `pytest` unit tests, `bundle validate`. All
   actions are pinned to commit SHAs; Dependabot bumps them, and the uv
   dependencies, weekly.
2. Merge to `main`: deploy to **test**.
3. Tag `v*`: deploy to **prod** (protect with a GitHub environment
   requiring reviewers).

The Databricks steps skip gracefully when the workspace secrets are not
configured, so the workflow stays green on repos without a wired
workspace.

Unit tests cover the metadata loader and run without a Spark runtime.
For integration testing, deploy to the dev target and run the pipeline
against sample events; validate results with the queries in
[Verifying the results](#verifying-the-results).

## Local development

```bash
uv sync                    # installs the dev tools (ruff, pyright, pytest)
uv run ruff check src tests
uv run pyright
uv run pytest

uv tool install pre-commit && pre-commit install   # gitleaks on every commit
```

## Requirements and notes

- Databricks with Unity Catalog; pipeline `channel: CURRENT`
  (serverless). The code uses `pyspark.pipelines` (`dp`) — the current
  API. On older runtimes, `import dlt` exposes the same surface under
  legacy names (`dlt.apply_changes` etc.).
- Debezium connector requirements: `capture.mode=change_streams_update_full`,
  pre-images enabled (`change_streams_with_pre_images`) if deletes must
  be applied, and files landed partitioned by topic
  (`mongodb.<db>.<collection>/...`) as this framework expects.
- Changing `scd_type` for an existing collection requires a full
  refresh of that collection's flows to rewrite history. Projection and
  gold changes are materialized views and recompute on the next update.

## Deployment guide

### Prerequisites

- Databricks CLI ≥ 0.230 authenticated to each workspace
  (`databricks auth login --host <workspace-url>`).
- Unity Catalog catalogs per environment (defaults: `cdc_dev`,
  `cdc_test`, `cdc_prod` — override via bundle variables).
- A landing volume per environment matching `landing_root` in
  `config/collections.yaml` (default
  `/Volumes/cdc_<env>/landing/debezium`), with Debezium events landing
  under `mongodb.<db>.<collection>/` prefixes.
- For test/prod: a service principal per environment with permission to
  create pipelines and write to its catalog.

### One-time setup

1. In `databricks.yml`, set `workspace.host` for each target (for dev
   you can use the git-ignored `resources/workspace.local.yml` instead,
   see above) and `run_as.service_principal_name` for test and prod.
2. Review `config/collections.yaml`: landing root, collection list,
   projections and expectations.
3. In GitHub, create `test` and `prod` environments, each with
   `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets (service-principal
   tokens), and require reviewers on `prod`.

### Deploy and run — dev (manual)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev debezium_mongodb_cdc
```

`mode: development` prefixes all resources with your username, so every
engineer gets an isolated copy against the dev catalog.

### Deploy — test and prod (CI only)

- Merge a PR to `main` → CI lints, tests, validates, then deploys the
  bundle to **test** as the test service principal.
- Tag a release (`git tag v1.0.0 && git push --tags`) → CI deploys the
  same commit to **prod** after the gated approval.

No human deploys to test or prod by hand; the bundle is the only
deployment path, so workspaces never drift from git.

### Post-deploy verification

```sql
-- Row counts arrived in silver
SELECT COUNT(*) FROM cdc_prod.cdc_mongo_fw.customers_silver;

-- Data-quality outcomes (dropped tombstones, unresolvable _ids, warns)
SELECT * FROM event_log(TABLE(cdc_prod.cdc_mongo_fw.customers_silver))
WHERE event_type = 'flow_progress';
```

For the full set of checks, see
[Verifying the results](#verifying-the-results).

### Rollback and re-processing

- **Code rollback**: deploy the previous git tag
  (`git checkout v0.9.0 && databricks bundle deploy -t prod`).
- **Data reprocessing**: run the pipeline with a full refresh for the
  affected collections (`databricks bundle run -t prod
  debezium_mongodb_cdc --full-refresh <name>_bronze,<name>_silver`).
- **Dev teardown**: `databricks bundle destroy -t dev` removes a
  developer's copy cleanly.

### Running on Databricks Free Edition

This pipeline was verified end-to-end on Databricks Free Edition
(serverless). Two of that edition's constraints require deviating from
the defaults above; everything else works unchanged.

1. **Use the built-in `workspace` catalog.** Free Edition uses Default
   Storage, so the CLI cannot create catalogs (`databricks catalogs
   create` fails with *"Metastore storage root URL does not exist"*).
   Instead of `cdc_dev`, override the catalog variable at deploy time —
   no code changes, this is what the bundle variable is for.

2. **Point the landing root at a volume in that catalog.** Create the
   schemas and volume, then override the `landing_root` bundle variable
   at deploy time (the committed `config/collections.yaml` stays
   unchanged; the override reaches the pipeline as the
   `cdc.landing_root` setting).

Full sequence:

```bash
# authenticate (browser OAuth; no PAT needed)
databricks auth login --host https://<workspace>.cloud.databricks.com \
  --profile free

# landing zone + target schema in the built-in catalog
databricks schemas create landing workspace --profile free
databricks schemas create cdc_mongo_fw workspace --profile free
databricks volumes create workspace landing debezium MANAGED \
  --profile free

# land some Debezium JSON events (one directory per topic)
databricks fs cp -r ./events/ \
  dbfs:/Volumes/workspace/landing/debezium/ --profile free

# deploy and run with the catalog and landing root overridden
databricks bundle validate -t dev --var="catalog=workspace" \
  --var="landing_root=/Volumes/workspace/landing/debezium" --profile free
databricks bundle deploy   -t dev --var="catalog=workspace" \
  --var="landing_root=/Volumes/workspace/landing/debezium" --profile free
databricks bundle run      -t dev debezium_mongodb_cdc --profile free
```

Tables land in `workspace.cdc_mongo_fw` (the `target_schema` variable).
The pipeline is already `serverless: true`, which is the only compute
Free Edition offers, so no compute changes are needed. Teardown:
`databricks bundle destroy -t dev --var="catalog=workspace"
--profile free`, then drop the `workspace.cdc_mongo_fw` schema (and
`workspace.landing`, unless the MySQL twin uses it too).

To skip the `--var` flags (and to run from VS Code), put the two
overrides in `resources/workspace.local.yml` as shown in the next
section.

The verification run covered three collections at once: SCD2 history
(update kept as a closed row plus a current row), the same-millisecond
update+delete tiebreak by oplog `ord`, pre-image deletes, a Kafka
tombstone, schema-drift documents (ObjectId and integer `_id`s, fields
present on one document only), a metadata-defined drop expectation
evaluated against the document, typed projections for both SCD types
(`$date` → timestamp, `$numberLong` → long), and a gold view joining
two collections on a VARIANT extraction — all confirmed correct. The
queries in [Verifying the results](#verifying-the-results) show each of
these.

### Running from VS Code

The [Databricks VS Code extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks)
deploys and runs the bundle from the editor; the pipeline runs on
serverless compute in the workspace, so Databricks Connect is not used
("Databricks Connect disabled" in the status bar is expected).
Optionally, [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools)
with the Databricks driver queries the resulting tables.

![VS Code: Databricks extension with the deployed pipeline, a SQLTools query over the gold view, and the pipeline run log](docs/images/vscode.png)

**One-time setup** (all files below are git-ignored):

1. Run `uv sync`, and authenticate the CLI to your workspace
   (`databricks auth login --host <workspace-url>`).
2. Create `resources/workspace.local.yml` with the workspace host. On
   Free Edition, also set the catalog and landing root there so no
   `--var` flags are needed:

   ```yaml
   targets:
     dev:
       workspace:
         host: https://<workspace>.cloud.databricks.com
       variables:                # Free Edition only
         catalog: workspace
         landing_root: /Volumes/workspace/landing/debezium
   ```

   Check it with `databricks bundle validate -t dev`. Don't move or
   edit this file while VS Code is open; the extension re-reads it and
   reports "Invalid host name" if the host disappears.
3. Create `.vscode/settings.json`:

   ```json
   {
     "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
     "python.testing.pytestEnabled": true,
     "python.testing.pytestArgs": ["tests"],
     "databricks.python.environmentSetup": "manual",
     "sqltools.connections": [
       {
         "name": "debezium mongodb cdc",
         "driver": "Databricks",
         "connectionMethod": "VS Code Extension (beta)",
         "path": "/sql/1.0/warehouses/<warehouse-id>",
         "catalog": "workspace",
         "schema": "cdc_mongo_fw"
       }
     ]
   }
   ```

   `environmentSetup: manual` stops the extension from rebuilding the
   uv-managed `.venv`. The SQLTools connection reuses the extension's
   login, so no token is stored; omit it if you don't use SQLTools.
   Set `catalog` to your `catalog` variable (`cdc_dev` outside Free
   Edition).

**Run:**

1. Open the repo folder itself (`code debezium-mongodb-cdc-pipeline`),
   not a parent folder; the extension looks for `databricks.yml` at the
   project root. If it reports that `databricks.yml` "already exists",
   run **Databricks: Select a Databricks project folder** from the
   command palette and pick the repo root.
2. Unit tests: **Testing** view → **Run Tests** (same as
   `uv run pytest`).
3. Databricks view → **Configuration** → **Login to Databricks** → pick
   your CLI profile. Configuration shows **Target: dev**.
4. **Bundle Resource Explorer** → **Pipelines** → hover
   `[dev <you>] debezium-mongodb-cdc-dev` → **Deploy the bundle and run
   the pipeline**. Dataset status appears under the pipeline; full
   output via **Databricks: Show bundle logs**.
5. Full refresh: same row → **Deploy the bundle and select pipeline
   tables for partial update** → select the tables and **Full
   refresh** → confirm.
6. Query the results: SQLTools icon in the activity bar → hover the
   **debezium mongodb cdc** connection → **Connect**. Save the queries
   from [Verifying the results](#verifying-the-results) in a `.sql`
   file (e.g. the git-ignored `.vscode/queries.sql`), put the cursor in
   a query and press `Cmd+E Cmd+E`; the result opens in a tab beside
   the editor.

### Verifying the results

Run these in the Databricks SQL Editor (with catalog `workspace` and
schema `cdc_mongo_fw` selected) or with SQLTools from VS Code. Outside
Free Edition, use your `catalog` and `target_schema` values.

```sql
-- 1. Row counts per silver table (customers is SCD2: all versions + current)
SELECT 'customers (all versions)' AS dataset, count(*) AS row_count FROM customers_silver
UNION ALL SELECT 'customers (current)', count(*) FROM customers_silver WHERE __END_AT IS NULL
UNION ALL SELECT 'orders', count(*) FROM orders_silver
UNION ALL SELECT 'products', count(*) FROM products_silver;

-- 2. SCD2 history of customers (document kept as JSON text in doc_json)
SELECT _id, doc_json:name::string AS name, doc_json:email::string AS email,
       __START_AT, __END_AT
FROM customers_silver
ORDER BY _id, __START_AT;

-- 3. Typed projections defined in config/collections.yaml
SELECT * FROM customers_typed ORDER BY _id;
SELECT * FROM orders_typed ORDER BY _id;

-- 4. Gold view defined in config/collections.yaml
SELECT * FROM gold_customer_order_counts ORDER BY customer_id;

-- 5. Data-quality results per flow and expectation (whole pipeline's event log)
SELECT e.origin.flow_name AS flow, ex.name AS expectation,
       SUM(ex.passed_records) AS passed, SUM(ex.failed_records) AS failed
FROM event_log(TABLE(workspace.cdc_mongo_fw.customers_silver)) e
LATERAL VIEW explode(from_json(
  e.details:flow_progress.data_quality.expectations,
  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
)) t AS ex
WHERE e.event_type = 'flow_progress'
GROUP BY ALL
ORDER BY flow, expectation;
```

What the verification run returned:

- **Counts:** `customers_silver` 6 rows, 2 of them current;
  `orders_silver` 2; `products_silver` 2.
- **SCD2 history:** Alice's email change is a closed row plus a current
  row. Bob's update and delete in the same millisecond are ordered by
  oplog `ord` (500, then 501), so his last version is closed. Carol is
  deleted through a pre-image. Dave has an integer `_id` (`42`); the
  others have ObjectIds. `__START_AT`/`__END_AT` are the
  `(ts_ms, ord)` sequence struct.
- **Typed projections:** `signup_at` comes from `$date` as a timestamp,
  `total_cents` from `$numberLong` as a long. Fields missing from a
  document come back NULL (schema drift).
- **Gold:** Alice has 2 orders, Dave has 0.
- **Expectations:** on `customers`, `valid_op` and `valid_document_id`
  drop the Kafka tombstone, which has neither an `op` nor an `_id`.
  The `positive_total` warning flags the refund order (`-500`) but
  keeps it, and `valid_email` drops nothing. The event log adds up
  every update, including full refreshes, so these counts grow with
  each run.
