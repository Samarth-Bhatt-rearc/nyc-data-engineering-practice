# nyc-data-engineering-practice

A Databricks Lakeflow data engineering pipeline that ingests the full public **NYC Yellow Taxi trip record** history (2009–2026) from the NYC Taxi & Limousine Commission (TLC), cleans it through a medallion (bronze/silver/gold) architecture, and surfaces it in an AI/BI dashboard. Deployed as a Databricks Asset Bundle (DAB).

## Architecture

```
NYC TLC (CloudFront)              landing job              bronze                 silver                    gold                      dashboard
  yellow_tripdata_*.parquet  -->  main.py          -->  yellow_tripdata_raw  -->  yellow_tripdata_silver -->  yellow_tripdata_gold        -->  Trip Summary page
  (209 monthly files,             (python_wheel_task,     (3 Auto Loader flows,     (cleaned payment/vendor/    yellow_tripdata_price_hikes -->  Price Hikes page
   2009-01 .. 2026-05)             lands into a UC          one per schema era)      flag codes, sentinel
                                   Volume, idempotent)                               defaults)
```

- **Landing** (`src/nyc_data_analysis_project/main.py`) — runs as the job's `python_wheel_task`, before the pipeline refresh. Downloads every published monthly file directly from TLC's CloudFront into a Unity Catalog Volume, sorted into 3 subfolders by schema era. Idempotent: re-runs only fetch months not already landed.
- **Bronze** (`src/nyc_data_analysis_project_etl/transformations/bronze/`) — ingests the landed files via Auto Loader. NYC TLC has reused the `yellow_tripdata_*` file naming across 3 incompatible column layouts over the years (2009 legacy, 2010 legacy, 2011+ modern — different names, and drifting types even within the modern era), so ingestion uses one streaming flow per era, each normalizing into a single canonical schema, all appending into one `yellow_tripdata_raw` table.
- **Silver** (`src/nyc_data_analysis_project_etl/transformations/silver/`) — cleans bronze: trims whitespace, normalizes payment type / vendor / store-and-forward-flag codes (which vary by era) via lookup tables, and fills sentinel defaults for legitimately-missing fields.
- **Gold** (`src/nyc_data_analysis_project_etl/transformations/gold/`) — two aggregate tables:
  - `yellow_tripdata_gold` — trip counts/distance by year, month, vendor, payment type.
  - `yellow_tripdata_price_hikes` — daily surcharge/fee totals (improvement, congestion, airport, CBD congestion) by pickup location, including whether each surcharge occurred and its percent contribution to the total.
- **Dashboard** (`src/dashboards/`) — a 2-page AI/BI (Lakeview) dashboard on top of the gold tables: "Trip Summary" and "Price Hikes".

Everything downstream of landing is incremental — Auto Loader only processes newly-landed files on each pipeline run, not the whole history.

## Project structure

- `databricks.yml` — bundle definition, targets (`dev`/`prod`), variables (`catalog`, `schema`, `warehouse_id`).
- `resources/` — bundle resource definitions: the job (`*_job.job.yml`), the pipeline (`*_etl.pipeline.yml`), the landing volume (`*_volume.yml`), and the dashboard (`*_dashboard.yml`).
- `src/nyc_data_analysis_project/` — the wheel package; `main.py` is the landing job's entry point.
- `src/nyc_data_analysis_project_etl/transformations/{bronze,silver,gold}/` — the Lakeflow pipeline's Python transformation files.
- `src/dashboards/` — the AI/BI dashboard definition (`.lvdash.json`), version-controlled alongside everything else.
- `tests/` — unit tests for the shared Python code.
- `fixtures/` — fixtures for test data.

## Prerequisites

Install these on your machine before you start:

- **[Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html)** — used for authentication and to deploy/run the bundle.
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** — this project's Python package manager (used instead of pip; dependencies are declared in `pyproject.toml`).
- **A JDK 17 install** — only needed to run the test suite locally (pytest spins up a real local Spark session, which needs a JVM). Newer JDKs (e.g. 26) are *not* compatible with this Spark version. On macOS: `brew install openjdk@17`.

You'll also need:

- Access to the Databricks workspace configured in `databricks.yml` (`workspace.host`), with permission to deploy bundles.
- The `catalog`/`schema` bundle variables (`databricks.yml`, currently `db_sandbox`/`practice_data_area`) must already exist in that workspace — the bundle doesn't create the catalog itself. Running against a different workspace/catalog/schema means editing those values in `databricks.yml` first.
- A running SQL warehouse for the dashboard (`warehouse_id` bundle variable, defaults to `0df70a5dd845ec7a`).

## Getting started

Exact steps to get this project running on your own machine, end to end:

1. **Clone the repo and `cd` into it:**
   ```
   $ git clone <this-repo-url>
   $ cd nyc-data-engineering-practice
   ```

2. **Authenticate the CLI to your Databricks workspace:**
   ```
   $ databricks configure --host <workspace-host>
   ```
   (prompts for a personal access token — see [CLI authentication](https://docs.databricks.com/dev-tools/cli/authentication.html) for other auth methods, e.g. OAuth via `databricks auth login`). You can also work in a Databricks workspace UI or an IDE extension (VS Code/Cursor) instead of the CLI — see [Bundles in the workspace](https://docs.databricks.com/dev-tools/bundles/workspace.html) / [VS Code extension](https://docs.databricks.com/dev-tools/vscode-ext.html) — but the steps below use the CLI directly.

3. **Install the project's Python dependencies:**
   ```
   $ uv sync --dev
   ```

4. **Validate and deploy a dev copy of the bundle:**
   ```
   $ databricks bundle validate
   $ databricks bundle deploy --target dev
   ```
   (`dev` is the default target, so `--target dev` is optional.) This deploys everything — the landing job, the Lakeflow pipeline, the landing volume, and the dashboard. In dev mode, resource names get a `[dev your_username]` prefix and schedules/triggers are paused by default. To deploy a production copy instead, use `databricks bundle deploy --target prod`.

5. **Run the job** — lands the parquet files into the volume, then refreshes the pipeline (bronze → silver → gold):
   ```
   $ databricks bundle run nyc_data_analysis_project_job
   ```
   The first run does a full historical backfill (209 monthly files, 2009-01 through 2026-05), so expect it to take a while; re-runs are incremental and only process newly-landed months.

6. **Publish the dashboard** so it's viewable (a deploy only updates the draft):
   ```
   $ databricks bundle summary --target dev
   ```
   to find the dashboard's resource URL/ID, then:
   ```
   $ databricks lakeview publish <dashboard-id> --warehouse-id <warehouse-id>
   ```
   After that, editing `src/dashboards/*.lvdash.json` and re-running `databricks bundle deploy` updates the draft, same as any other resource — re-publish after each such change for viewers to see it.

7. **(Optional) Run the test suite locally.** This needs a *separate* venv from the one step 3 created — `databricks-connect` (in the `dev` dependency group) and plain `pyspark` (needed for tests' local Spark session) both occupy the `pyspark` import namespace and can't coexist in the same environment:
   ```
   $ UV_PROJECT_ENVIRONMENT=.venv-test uv sync --group test --no-group dev
   $ .venv-test/bin/pytest
   ```
   Run tests via `.venv-test/bin/pytest` directly, not `uv run pytest` — `uv run` auto-resyncs the `dev` group back in before running (reinstalling `databricks-connect` over `pyspark`), even with `--no-group dev`/`--only-group test` passed. If `JAVA_HOME` doesn't already point at a JDK 17, set it for the run, e.g. `JAVA_HOME=$(brew --prefix openjdk@17) .venv-test/bin/pytest` on macOS.
