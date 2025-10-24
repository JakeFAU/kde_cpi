# KDE CPI

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
- `pgadmin`: pgAdmin4 UI reachable at http://localhost:5050 (default creds: `jacob.bourne@gmail.com` / `admin123`)

To launch the stack:
```bash
export POSTGRES_USER=kde_cpi \
       POSTGRES_PASSWORD=kde_cpi \
       POSTGRES_DB=kde_cpi
docker compose up -d
```

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

Examples:

```bash
# Load the complete CPI dataset (truncates existing tables first)
kde-cpi load-full

# Refresh latest observations only
kde-cpi update-current

# Grab a JSON snapshot without touching the database
kde-cpi fetch-dataset --current-only --output out/current.json
```

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
