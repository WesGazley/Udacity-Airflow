# Sparkify Lakehouse — Data Pipelines with Airflow

Event-driven medallion lakehouse (`raw` → `transactions` → `analytics`) on Iceberg,
orchestrated by Airflow Assets, built for the "Data Pipelines with Airflow" project.

## Provenance

Every file in this repo is copied verbatim from the Udacity workspace, or is a
workspace skeleton (`#### YOUR CODE HERE` markers) with every blank filled in while
preserving all given code unchanged:

- `setup/run_pipeline.py`, `setup/setup_s3_data.py` — complete starter files.
- `raw/dag.py`, `raw/glue_script.py`, `transactions/dag.py`,
  `transactions/glue_script.py`, `analytics/dag.py`, `analytics/glue_script.py` —
  skeletons with blanks filled in.
- `transactions/sql/*.sql`, `analytics/sql/*.sql` — the pre-written SQL, copied as-is
  (not authored here).
- `lakehouse_infrastructure.yaml` — copied verbatim.

Note on `raw.logs` columns: the raw JSON source uses camelCase keys (`userId`,
`firstName`, `sessionId`, `itemInSession`, `userAgent`, ...). Spark's JSON schema
inference preserves that casing, but Spark SQL resolves column names
case-insensitively by default, so the SQL files reference them lowercase
(`userid`, `firstname`, `sessionid`, `iteminsession`, `useragent`).

## Architecture

```
run_pipeline (manual trigger, param: data_interval)
    │  Asset("raw_ingestion_pending")        {data_interval}
    ▼
raw                                          (schema-driven discovery + Iceberg ingest)
    │  Asset("raw_ingestion_complete")       {data_interval}
    ▼
transactions                                 (staged MERGE, dependency-ordered promotion)
    │  Asset("transactions_complete")
    ▼
analytics                                    (full CREATE OR REPLACE recompute)
```

Triggering `run_pipeline` once cascades through all three downstream DAGs automatically
via Asset scheduling. Every table lives in the Glue Data Catalog under one shared
Iceberg/Spark catalog named `iceberg` (`iceberg.raw.*`, `iceberg.transactions.*`,
`iceberg.analytics.*`), backed by `s3://<bucket>/iceberg-warehouse/`.

### `raw/` — schema-driven ingestion

- `capture_landing_keys` lists `s3://<bucket>/data_intervals/<interval>/` and treats
  every subfolder as a table — no table or column name is hardcoded anywhere in
  `raw/dag.py` or `raw/glue_script.py` other than the `data_interval` partition column.
- One reusable Glue job is dynamically mapped once per discovered table
  (`GlueJobOperator.partial(...).expand_kwargs(table_keys.map(...))`). Spark infers the
  JSON schema and writes into `iceberg.raw.<table>` — `.create()` on first run,
  `.overwritePartitions()` after, so re-running the same interval is idempotent and
  never touches other intervals.
- `SQLCheckOperator` (via `athena_default`) confirms `raw.logs` and `raw.songs` both
  have rows for the interval before `raw_ingestion_complete` is emitted.

### `transactions/` — normalized, deduplicated, dependency-ordered

- Six tables, each declared in the `TABLES` list in `transactions/dag.py` with its
  `upsert_keys` (primary key) and `depends_on` (promotion order): `artists`, `users` →
  `songs` (after artists) → `song_versions` (after songs), `user_levels` (after users),
  and `events` last (after users, songs, artists, song_versions).
- **How promotion works**: each table's `.sql` file is a plain `SELECT` that stages this
  interval's rows from `iceberg.raw.*` (no `MERGE`/`INSERT` in the SQL itself). The DAG
  renders it (Jinja, via `ti.task.render_template`), uploads it to S3, and
  `transactions/glue_script.py` — one generic, table-agnostic executor driven entirely
  by the `--config` JSON (`table`, `sql`, `upsert_keys`, `partition_keys`) — runs it,
  drops duplicates on `upsert_keys`, and `MERGE INTO`s the result (`UPDATE SET *` /
  `INSERT *`) into `iceberg.transactions.<table>`, creating the table on first run.
- `SQLCheckOperator` checks `COUNT(*) = COUNT(DISTINCT <upsert_keys>)` per table, plus a
  join-quality check on `events.version_id`, before `transactions_complete` is emitted.

### `analytics/` — full-snapshot fact tables

- `analytics/dag.py` loops over every file in `analytics/sql/` (no table names
  hardcoded), uploads each rendered query, and runs it through
  `analytics/glue_script.py` — the same generic S3-SQL pattern, but instead of merging
  it calls `.writeTo(target_table).using("iceberg").createOrReplace()`, a full
  replacement of the previous snapshot. `analytics/glue_script.py` contains no SQL and
  no append/insert operations of its own — the recompute semantics live entirely in
  that one `.createOrReplace()` call.

## Setup (see the project brief for full detail)

1. Deploy `lakehouse_infrastructure.yaml` via CloudFormation (creates the `raw`,
   `transactions`, `analytics` Glue databases; the `dev-lakehouse-glue-role` IAM role;
   the `dev-lakehouse` Athena workgroup; and the project S3 bucket).
2. In the Airflow UI, create `aws_default` and `athena_default` connections.
3. Set the `s3_bucket` Airflow Variable to the bucket CloudFormation created.
4. Unpause all four DAGs, then trigger `setup_s3_data` to seed the landing bucket.
5. Trigger `run_pipeline`, pick an interval, and watch `raw` → `transactions` →
   `analytics` fire automatically.

Repeat step 5 for each of the three test intervals — every table is upserted/recomputed
idempotently, so re-running an interval reproduces the same final state.

## Repo layout

```
lakehouse_infrastructure.yaml
setup/            run_pipeline.py, setup_s3_data.py
raw/              dag.py, glue_script.py
transactions/     dag.py, glue_script.py, sql/*.sql
analytics/        dag.py, glue_script.py, sql/*.sql
```
