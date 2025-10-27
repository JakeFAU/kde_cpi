# KDE-CPI Service Manual

*A practical guide to understanding, deploying, and contributing to the Kernel Density Estimation CPI project.*

---

## 1. Overview

**KDE-CPI** is a command-line application and Python library that computes inflation metrics using non-parametric statistics, specifically **kernel density estimation (KDE)**. It transforms raw Bureau of Labor Statistics (BLS) CPI flat files into an interpretable, database-backed dataset, then estimates the **mode** of the price change distribution as an alternative inflation measure.

The KDE mode captures where the *typical* price change lies, rather than averaging extremes. It’s statistically robust, economically intuitive, and reproducible through a fully transparent pipeline.

---

## 2. System Architecture

```text
        ┌──────────────────────┐
        │  BLS CPI Flat Files  │
        └─────────┬────────────┘
                  │ ingest
                  ▼
         ┌──────────────────┐
         │ PostgreSQL Store │
         └─────────┬────────┘
                   │ compute
                   ▼
          ┌──────────────────┐
          │  KDE Analytics   │
          │  (mode, stats)   │
          └─────────┬────────┘
                    │ export
                    ▼
           ┌──────────────────┐
           │ JSON + PNG Plots │
           └──────────────────┘
```

### Core components

* **Ingestion:** Downloads and parses BLS CPI datasets.
* **Storage:** Normalized tables in PostgreSQL for items, areas, and observations.
* **Computation:** Statistical analysis in Python, leveraging pandas and NumPy.
* **Visualization:** Density plots, histograms, and summary exports in JSON.

---

## 3. Installation

### Prerequisites

* Python 3.11+
* PostgreSQL 13+

### Installation via pip

```bash
git clone https://github.com/yourusername/kde-cpi.git
cd kde-cpi
pip install -e .
```

### Configuration

Set environment variables or use CLI options:

```bash
export KDE_CPI_DSN=postgresql://user:pass@localhost:5432/cpi_db
export KDE_CPI_SCHEMA=cpi_app
```

---

## 4. Command-Line Interface

All entry points are subcommands under the `kde-cpi` CLI.

### Fetch CPI Data

```bash
kde-cpi fetch-dataset --current-only
```

Downloads and parses the latest CPI flat files. Use `--data-file` for specific partitions.

### Analyze Dataset

```bash
kde-cpi analyze --source database --output-dir out/
```

Computes KDE-based inflation metrics and generates density plots.

### Compute Summary Only

```bash
kde-cpi compute --source database --output out/summary.json
```

Outputs modal inflation statistics as structured JSON.

### Database Loading

```bash
kde-cpi load-full --dsn postgresql://user:pass@host/db --schema cpi_app
```

Loads full CPI history into PostgreSQL, optionally truncating existing data.

### Schema Management

```bash
kde-cpi ensure-schema --dsn postgresql://user:pass@host/db
```

Ensures CPI tables exist before ingestion.

### Series Locks & Small-Sample Controls

Analytics-oriented commands (`analyze`, `compute`, `panel`, `metrics-timeseries`) support optional guards to target specific slices of CPI series and manage sample reliability:

* `--series-lock KEY=VALUE` (repeatable) rebuilds the candidate set using metadata components such as `area_code`, `seasonal`, `base_code`, or `item_code`.
* `--min-sample-size N` emits a warning whenever a group or period drops below `N` observations (use `0` to disable).
* `--skip-small-samples` tells the CLI to drop those undersized groups entirely after warning, leaving the default behavior unchanged when omitted.

These controls make it easy to contrast seasonally adjusted vs. not seasonally adjusted series, isolate particular geographies, or ensure KDE outputs only appear when statistical coverage is adequate.

---

## 5. Data Model Summary

| Table            | Description                                  |
| ---------------- | -------------------------------------------- |
| **areas**        | Geographical areas used in CPI reporting     |
| **items**        | Goods and services tracked across time       |
| **series**       | CPI series identifiers combining area + item |
| **observations** | Time-stamped CPI values with weights         |
| **metadata**     | Internal metadata and versioning             |

Each observation includes: `series_id`, `year`, `period`, `value`, and normalized weight.

---

## 6. Statistical Workflow

1. **Load Observations** — The system loads raw CPI data into memory, weighted by expenditure shares.
2. **Normalize & Filter** — Missing values and unselectable items are removed.
3. **Compute KDE** — A Gaussian KDE is applied over inflation rates \(x_i\) with weights \(w_i\):

$$
\hat{f}_h(x) = \frac{1}{h} \sum_i w_i K\!\left( \frac{x - x_i}{h} \right),
\quad
K(u) = \frac{1}{\sqrt{2\pi}} e^{-u^2 / 2}.
$$

4. **Bandwidth Selection** — Weighted Scott’s rule:

$$
h = 0.9 \min\!\left(\sigma, \frac{\mathrm{IQR}}{1.34}\right)
n_{\mathrm{eff}}^{-1/5}, \quad
n_{\mathrm{eff}} = \frac{1}{\sum_i w_i^2}.
$$

5. **Locate Mode** — \( x^* = \arg\max_x \hat{f}_h(x) \)

6. **Summarize** — Output includes mean, median, trimmed mean, KDE mode, skewness, kurtosis, and effective sample size.

---

## 7. Output Artifacts

| File Type         | Description                               |
| ----------------- | ----------------------------------------- |
| **JSON**          | Structured summary of computed statistics |
| **PNG**           | Plots comparing mean, median, and mode    |
| **CSV / Parquet** | Optional tabular export for panel data    |

Example output snippet:

```json
{
  "weighted_mean": 0.034,
  "weighted_median": 0.029,
  "kde_mode": 0.027,
  "weighted_std": 0.012,
  "effective_sample_size": 2218
}
```

---

## 8. Contributing

Contributions are welcome—particularly for:

* Optimized bandwidth selection and faster KDE evaluation
* CLI usability and help text improvements
* New visualization modules (e.g., time series of modal inflation)

Fork the repository and submit a PR with clear commit messages and test coverage.

### Example development flow

```bash
git checkout -b feature/faster-kde
pytest --cov=kde_cpi tests/
git push origin feature/faster-kde
```

---

## 9. Troubleshooting

| Issue                               | Cause                          | Resolution                                |
| ----------------------------------- | ------------------------------ | ----------------------------------------- |
| `ModuleNotFoundError: structlog`    | Dependency missing             | Run `pip install structlog attrs`         |
| `No data was collected` in coverage | No tests imported main package | Ensure test files import `kde_cpi`        |
| `psycopg2.errors.UndefinedTable`    | Schema not initialized         | Run `kde-cpi ensure-schema` before load   |
| CLI command hangs                   | BLS download delay             | Use `--current-only` for faster test runs |

---

## 10. Summary

KDE-CPI converts the complexity of national price data into a reproducible analytical workflow. It replaces fragile summary metrics with a full density-based picture of inflation, enabling analysts to see where *most* prices actually move.

By emphasizing transparency, mathematical rigor, and open contribution, KDE-CPI aims to make advanced inflation analytics accessible to everyone—from economists to engineers.
