# KDE CPI

[![docs](https://img.shields.io/badge/docs-latest-blue)](https://jakefau.github.io/kde_cpi/)

Tools for downloading Bureau of Labor Statistics Consumer Price Index (CPI) flat files, loading them into PostgreSQL, and generating useful summaries via a unified Click-based CLI.

## Prerequisites

- Python 3.11+
- `pip`/`venv` (or preferred environment manager)
- Docker + Docker Compose (needed for the bundled PostgreSQL/pgAdmin stack)

## Getting Started

1. **Clone and enter the repo**

   ```bash
   git clone https://github.com/your-org/kde_cpi.git
   cd kde_cpi
   ```

2. **Create and activate a virtual environment**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -e .
   # Optional extras
   pip install -e .[dev,test]
   ```

## Database via Docker Compose

The included `docker-compose.yml` spins up:

- `postgres`: PostgreSQL 18 with schema bootstrap scripts mounted from `init.d/`
- `pgadmin`: pgAdmin4 UI reachable at <http://localhost:5050> (default creds: `jacob.bourne@gmail.com` / `admin123`)

To launch the stack:

```bash
export POSTGRES_USER=kde_cpi \
       POSTGRES_PASSWORD=kde_cpi \
       POSTGRES_DB=kde_cpi
docker compose up -d
```

> **Note:** The stack uses PostgreSQL 18, which expects persisted data at `/var/lib/postgresql`. If you previously ran an older version that stored data under `/var/lib/postgresql/data`, remove or rename the old Docker volume (for example via `docker compose down -v` or `docker volume rm kde_cpi_postgres-data`) before starting the containers to avoid initialization errors.

After the containers are healthy you can connect to PostgreSQL at `postgresql://kde_cpi:kde_cpi@localhost:5432/kde_cpi`.

## CLI Usage

Installing the project exposes the `kde-cpi` command. All commands accept `--dsn` and `--schema` (default `public`), or you can set environment variables:

```bash
export KDE_CPI_DSN="postgresql://kde_cpi:kde_cpi@localhost:5432/kde_cpi"
export KDE_CPI_SCHEMA="public"
```

Key commands:

| Command | Purpose |
| ------- | ------- |
| `kde-cpi fetch-dataset [--current-only] [--data-file cu.data.0.Current …] [--output path.json]` | Download CPI flat files and optionally write a JSON snapshot. |
| `kde-cpi load-full [--no-truncate] [--data-file …]` | Ingest the full CPI history into PostgreSQL. |
| `kde-cpi update-current` | Merge only the current-year partition into the database. |
| `kde-cpi ensure-schema` | Create the CPI tables if they do not exist. |
| `kde-cpi sync-metadata [--current-only] [--data-file …]` | Refresh mapping tables and series definitions without touching observations. |
| `kde-cpi analyze [--group-by ...] [--source database|flatfiles] [...]` | Compute YoY growth distributions, render KDE/histogram plots, and save summaries (database by default). |
| `kde-cpi compute [--date YYYY-MM] [--group-by ...]` | Produce a JSON summary (no plots) for a single month/grouping. |
| `kde-cpi panel --start YYYY-MM --end YYYY-MM --export out/panel.parquet` | Build a tidy panel of metrics across many months (CSV or Parquet). |
| `kde-cpi metrics-timeseries --start YYYY-MM --end YYYY-MM --export out/metrics.csv` | Export a single time series of KDE metrics to study stability over time. |

Examples:

```bash
# Load the complete CPI dataset (truncates existing tables first)
kde-cpi load-full

# Refresh latest observations only
kde-cpi update-current

# Grab a JSON snapshot without touching the database
kde-cpi fetch-dataset --current-only --output out/current.json

# Generate charts grouped by display level using PostgreSQL data
kde-cpi analyze --group-by display-level --selectable-only

# Bucket by item-code length using flat files
kde-cpi analyze --source flatfiles --group-by item-code-length --output-dir out/analytics

# JSON summary for a specific month
kde-cpi compute --date 2024-12 --group-by item-code-length --output out/summary_2024-12.json

# Panel export between two dates (writes Parquet)
kde-cpi panel --start 2023-01 --end 2024-12 --group-by display-level --export out/kde_panel.parquet

# Overall KDE metrics as a tidy time series (CSV)
kde-cpi metrics-timeseries --start 2020-01 --end 2024-12 --export out/mode_timeseries.csv
```

### Logging

Structured logging is powered by `structlog` and controllable via CLI switches or env vars:

```bash
# Console-friendly logs at debug level
kde-cpi --log-level debug --log-format console fetch-dataset --current-only

# JSON logs, configured globally
export KDE_CPI_LOG_LEVEL=info
export KDE_CPI_LOG_FORMAT=json
kde-cpi load-full --no-truncate
```

Error and warning events include stack traces, while debug logs trace HTTP fetches, parser stages, and pipeline orchestration.

### Analysis Outputs

`kde-cpi analyze` pulls CPI data from PostgreSQL by default (pass `--source flatfiles` to re-download from BLS), computes year-over-year growth for every series, and generates density + histogram plots per group. Artifacts land under a timestamped directory such as `out/analysis_display-level_20250309_154212/`, containing:

- `summary.json` – top-level metadata, counts, and per-group stats
- `group_<label>/density.png` & `histogram.png` – visualizations for each bucket
- `group_<label>/summary.json` – stats + sample series for the bucket

Use `--group-by display-level` (default) to bucket by CPI item display levels, or `--group-by item-code-length` to bucket by the length of CPI item codes (4-char vs 6-char, etc.). The legacy `series-name-length` synonym still works but will be removed later. Set `--include-unselectable` if you want to include non-published CPI components.

`kde-cpi compute` shares the same grouping and source flags but skips plotting, returning a JSON payload with full statistics plus representative components. `kde-cpi panel` iterates month-by-month, flattening the summaries into a tidy table (CSV or Parquet) so you can downstream filter, chart, or feed dashboards. When you just need one consolidated view of how the core KDE metrics evolve through time, `kde-cpi metrics-timeseries` emits a pandas-friendly CSV/Parquet with one row per month covering the mode, mean, median, trimmed mean, dispersion, and higher moments.

## Development

- Run tests: `pytest`
- Lint/format: `ruff check . && ruff format .`

Shut down the Docker resources when finished:

```bash
docker compose down
```

## Troubleshooting

- **SSL or auth errors:** ensure your `KDE_CPI_DSN` matches the Docker Compose credentials or any external database configuration.
- **Schema issues:** rerun `kde-cpi ensure-schema` to create/update tables before loading data.
- **Large downloads:** the CPI flat files are sizeable; for quick smoke tests use `--current-only` or specify a limited `--data-file`.
