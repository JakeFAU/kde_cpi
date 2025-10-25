# KDE-Mode Inflation: Roadmap of Enhancements

> Scope: production-grade features for computing, validating, and publishing KDE-mode inflation at any aggregation level, with longitudinal analysis across your full component time series in Postgres.

---

## 1 Methodology & Controls

* **Bandwidth strategy**

  * Implement plug-in/CV bandwidth selectors; expose a fixed default and an **adaptive bandwidth** option (Abramson-style) for skewed/tail-heavy baskets.
  * Acceptance: selector chosen via flag; report selected `h` and effective sample size in metadata.

* **Support-aware kernels**

  * Add **bounded-support** treatment for percentage changes (lower bound −100%), or perform KDE on **log price relatives** (log ΔP) and map back to percent.
  * Acceptance: users can toggle `domain={"percent","log_relative"}`; unit tests show mode invariance under monotone transforms.

* **Weighting schemes**

  * Allow Laspeyres, Paasche, Törnqvist/Fisher weights within KDE (component-level weights applied to kernels).
  * Acceptance: outputs include `weighting_scheme` and reproducible results for each scheme.

* **Trim/winsor options**

  * Optional winsorization or robust scaling prior to KDE to stabilize extreme outliers.
  * Acceptance: summary shows % mass trimmed and its effect on mode vs median.

---

## 2 Uncertainty, Stability, and Diagnostics

* **Bootstrap CIs for the mode**

  * Weighted resampling of components to produce percentile or BCa intervals for the mode.
  * Acceptance: `mode_ci_low`, `mode_ci_high` fields; visualization shows a ribbon.

* **Sensitivity panel**

  * Grid over bandwidths and trims; plot mode surface vs `(h, trim)`.
  * Acceptance: single function returns a small dataframe + plot to assess robustness.

* **Commutativity check**

  * Compare: (A) headline from raw components vs (B) headline by aggregating category-level modes. Flag divergences.
  * Acceptance: report `commutativity_gap` and top contributors.

* **Shape diagnostics**

  * Publish component dispersion metrics (IQR, variance of log relatives), multimodality test (e.g., Hartigan’s Dip).
  * Acceptance: add `dispersion_index`, `dip_pvalue` to outputs.

---

## 3 Time-Series & Regime Analysis (leveraging Postgres)

* **Rolling computation**

  * Compute mode, mean, median, and dispersion monthly for each level; store in a `kde_metrics` table (parquet export).
  * Acceptance: function `compute_kde_panel(level, start, end)` writes/returns a tidy panel.

* **Mode–median spread & tails index**

  * Track `mode_minus_median`, `mode_minus_trimmed_mean`, and a tails pressure index (mass outside ±x%).
  * Acceptance: plots + alert when spreads exceed historical bands.

* **Change-point detection**

  * Add Bayesian online change point or Bai-Perron tests on the **mode** and **spread** series.
  * Acceptance: `regime_start` markers and a summary table.

* **Attribution over time**

  * Decompose shifts in the mode into: (i) composition effects (weights) vs (ii) within-category price changes (Shapley-style or Oaxaca-Blinder analog).
  * Acceptance: quarterly attribution report listing top drivers.

---

## 4 Comparability & Benchmarks

* **Cleveland-style metrics**

  * Compute median CPI and trimmed-mean with matching baskets for apples-to-apples comparison.
  * Acceptance: unified chart with all series; table of pairwise correlations and RMSE vs headline.

* **Synthetic truth tests**

  * Generate mixture distributions with known modal locations; verify estimator bias/variance across n, skew, kurtosis.
  * Acceptance: test suite passes with target error bounds.

---

## 5 Data Pipeline & Interfaces

* **CLI / API**

  * `kde-cpi compute --level=headline --date=YYYY-MM --scheme=fisher --domain=log_relative --h=auto --bootstrap=1000`
  * `kde-cpi panel --level=division --start=YYYY-MM --end=YYYY-MM --export=parquet`
  * Acceptance: commands return exit code 0; outputs written with full metadata.

* **Postgres integration**

  * SQL views for component pulls; idempotent jobs that cache per-date results; `upsert` on `(date, level, scheme, domain, hhash)`.
  * Acceptance: reruns are deterministic; checksum recorded for inputs.

* **Artifacts & metadata**

  * Standard schema:
    `date, level, scheme, domain, bandwidth, n_components, mode, mode_ci_low, mode_ci_high, mean, median, trimmed_mean, dispersion_index, commutativity_gap, dip_pvalue, created_at, git_sha`
  * Acceptance: parquet + CSV exports; schema versioned.

---

## 6 Visualization & Reporting

* **Production chart set**

  * Headline line chart with CI ribbon; panel of mode-median spread; rug plot at selected dates; top ± contributors at the **mode**.
  * Acceptance: single function returns a matplotlib figure bundle ready for reporting.

* **Dashboard**

  * Minimal Streamlit/Plotly app: level selector, bandwidth knob, date slider, regime markers.
  * Acceptance: loads in <2s on laptop; queries cached.

* **Narrative summary**

  * Auto-generate a 3-bullet “typical price story this month,” sourced from contributors near the mode.
  * Acceptance: templated text with guardrails to avoid tail events masquerading as typical.

---

## 7 Testing, QA, and Performance

* **Unit & property tests**

  * Mode invariance under monotone transforms; reproducibility with fixed seeds; CI bandwidth monotonicity checks.
  * Acceptance: ≥90% coverage on estimator code.

* **Performance**

  * Vectorized KDE with FFT where possible; parallel bootstrap; memoize per-date kernels.
  * Acceptance: monthly headline compute (w/ 1k bootstrap) completes under target time on commodity hardware.

* **Repro & governance**

  * Pin dependencies; `pyproject.toml`; deterministic RNG; record `git_sha` in outputs.
  * Acceptance: fresh clone reproduces published figures end-to-end.

---

## 8 Documentation

* **Method note**

  * Short PDF/MD: why mode, estimator details, weighting, uncertainty, and limitations.
* **User guide**

  * “When to prefer KDE-mode vs median/trimmed-mean” with toy distributions and counterexamples.
* **Readme quickstart**

  * 5-minute path from Postgres to chart; CLI examples; FAQ on bandwidth, outliers, and weights.

---

### Optional “Stretch” Ideas

* **Hierarchical shrinkage** of bandwidth across levels (share information).
* **Nowcasting**: partial-month mode using early reporters; backfill monitor.
* **Forecast utility tests**: does mode improve short-horizon headline prediction vs trimmed-mean?
