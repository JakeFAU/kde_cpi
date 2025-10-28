# Beware of Geeks Bearing Gifts

This didn’t start as an effort to help my old inflation-trading friends, but somehow it ended up as one. Ironically, the infrastructure I built to run the analysis may turn out to be more useful than the results themselves.

It began when Apple News showed me an article about a recent inflation print. I have no idea why the algorithm thought I’d care—I don’t—but from its perspective, it worked: I clicked. It was dreadful. Embarrassingly bad. It’s been about ten years since I last read one of those articles, and they’ve only gotten worse.

It infuriated me—not because I still trade inflation, but because I’ve seen how far machine learning has come in the past decade, while the Bureau of Labor Statistics (BLS) still seems to be using methods developed when disco was cutting-edge.

---

## Measurement Error

The basic problem—besides the arithmetic gymnastics—is that the inflation categories used by both the media and the Federal Open Market Committee (FOMC) are absurd. Maybe they made sense in 1970, but we’ve learned a lot since then.

The Cleveland Fed has built more sophisticated variants, but they too have their flaws. So fine, I thought: let’s build *one metric to rule them all, and in the darkness (of advanced math) bind them.*

Here are the classics, and why they all miss the mark:

* **Total CPI** – It has the virtue of being *total*, which is more than the others can claim. Unfortunately, it’s a lousy predictor of future inflation and would likely lead to procyclical monetary policy if taken literally.
* **Core CPI** – This one’s a joke, and someone should say it. There’s a reasonable argument for excluding energy—there’s little the FOMC can do about oil prices—but excluding food makes no sense. U.S. consumer food prices have almost no correlation with raw commodity prices. They’re driven by shipping, storage, processing, labor, and marketing—exactly the forces that reflect general inflation pressures. Food shouldn’t be ignored; it’s integral.
* **Cleveland Fed 16% Trimmed Mean** – Smarter than “core” because it removes *actually volatile* components instead of categories guessed to be volatile in the 1970s. But why 16% (8% from each tail)? Why not 14% or 18%? Even if that trim gave the strongest forward correlation in one dataset, that’s still a sample of one.
* **Cleveland Fed Median CPI** – Smarter still, since it resists outliers, but it collapses when the distribution is multimodal. Two peaks, one median: that’s not representation, it’s flattening.

---

## KDE Mode — Letting the Data Speak

Imagine standing in a dark field with a flashlight, shining it once for every price change in the economy. Where your beams overlap, the light brightens—those are the price movements that happen most often. That luminous landscape is what a **Kernel Density Estimate (KDE)** builds: a smooth probability surface from the scattered points of real data.

### Step 1: The Core Idea

Traditional inflation measures like the mean or median collapse all prices into one number. KDE does something radical for economics—it refuses to simplify too early. Instead of assuming a shape (bell curve, log-normal, whatever), it estimates the shape *from the data itself*. Each data point adds a “bump” of probability, described by a **kernel function**—often a Gaussian curve centered on that observation. Summing those bumps yields a continuous estimate of the underlying distribution.

Formally:
$$
\hat{f}(x) \;=\; \frac{1}{n\,h}\,\sum_{i=1}^{n} K\!\left(\frac{x - x_i}{h}\right)
$$

where:

* (x_i) are observed year-over-year price changes,
* (K) is the kernel (the bump shape),
* (h) is the bandwidth (how wide each bump spreads), and
* (n) is the number of data points.

The **bandwidth** controls the trade-off: too small, and the distribution looks noisy; too large, and it looks bland. KDE balances that tension to reveal the real structure—fat tails, asymmetry, even multiple peaks.

### Step 2: Finding the Mode

Once you have the KDE curve, the **mode**—the highest point—marks the most common inflation rate across all goods and services. That’s not an average. It’s the *consensus reality* of the economy at that moment, the inflation rate most consumers actually experience.

In the `kde_cpi` implementation, this is computed directly from the smoothed distribution of year-over-year price changes, grouped by display level or other series dimensions drawn from your normalized CPI database.

Mathematically:
$$
\text{KDE Mode} \;=\; \underset{x}{\arg\max}\;\hat{f}(x)
$$

### Step 3: Why It Works Better

1. **Nonparametric honesty:** No assumptions about normality or symmetry—unlike trimmed means or medians that pretend the distribution’s simple.
2. **Robustness:** Outliers don’t dominate because each observation contributes locally. The shape emerges organically.
3. **Granularity:** Because it runs directly on the full CPI microstructure (every item-area-series combination stored in your schema), it respects the heterogeneity of the economy rather than smoothing it away.
4. **Interpretability:** The mode tells us what *most prices* are doing, not just the average of all spending weights.

### Step 4: Comparison — Why KDE Mode Wins

| Measure                | Core Method                | Sensitivity to Outliers | Handles Multimodality? | Intuitive Meaning                                   |
| ---------------------- | -------------------------- | ----------------------- | ---------------------- | --------------------------------------------------- |
| **Mean CPI**           | Weighted average           | Very high               | ❌                      | "Average" inflation (distorted by large categories) |
| **Median CPI**         | 50th percentile            | Moderate                | ❌                      | "Middle" inflation, but blind to multiple peaks     |
| **Trimmed Mean CPI**   | Drops tails                | Low                     | ❌                      | "Average" after arbitrary censorship                |
| **KDE Mode Inflation** | Nonparametric density peak | Low                     | ✅                      | "Most common" inflation rate—data-driven consensus  |

Where older measures silence data that doesn’t fit a preconception, KDE listens to the whole orchestra. It doesn’t tell policymakers what they *want* to see; it tells them what the data is *actually playing*. And in the era of pandemic supply shocks and rent distortions, that difference isn’t statistical—it’s philosophical.

---

## `kde_mode`: Turning the Idea into a Tool

`kde_mode` is a Python package that fetches CPI data directly from the BLS, computes the KDE of component price changes, and extracts the mode.
It’s open source, available on [GitHub](https://github.com/JakeFAU/kde_cpi), and installable via pip:

```bash
pip install kde_mode
```

It runs from the command line or as an importable library. Documentation is [Docs](https://jakefau.github.io/kde_cpi/), but it’s designed to be intuitive—data in, distribution out.

---

## Gift 1: A Modern Database from 1950s Files

This is the first gift `kde_mode` gives you: a full modernization of the BLS’s creaky infrastructure.
The BLS still distributes CPI data as dozens of flat files—a format better suited for a mainframe than modern analytics. `kde_mode` automates the entire ingestion process: downloading, decoding, and stitching every partition into a relational **PostgreSQL** database. The result is clean, normalized, and query-ready for any analytical or machine learning workflow.

There’s even a `docker-compose.yml` that builds the database and launches **pgAdmin** for browsing. You can point it at your own DSN if you prefer. It’s a small quality-of-life revolution for anyone who’s ever parsed BLS flat files by hand.

### The Database

The loader builds a small, tidy star-ish schema around BLS CPI series and their observations. Here’s the lay of the land:

```text
          ┌────────────────┐        ┌─────────────────┐
          │    cpi_area    │        │    cpi_item     │
          │  area_code PK  │        │ item_code  PK   │
          │  area_name     │        │ item_name       │
          └──────┬─────────┘        │ display_level   │
                 │                  │ selectable      │
                 │                  │ sort_sequence   │
                 │                  └─────────┬───────┘
                 │                            │
                 │        ┌───────────────────▼───────────────────┐
                 └────────►                 cpi_series            │
                          │ series_id  PK                          │
                          │ area_code  → cpi_area.area_code        │
                          │ item_code  → cpi_item.item_code        │
                          │ seasonal (S/U)                          │
                          │ periodicity_code (R, etc.)             │
                          │ base_code, base_period                 │
                          │ begin_year/period, end_year/period     │
                          └───────────────┬────────────────────────┘
                                          │
                              ┌───────────▼───────────┐
                              │     cpi_observation   │
                              │ (series_id,year,period) PK
                              │ value NUMERIC         │
                              │ footnotes TEXT[]      │
                              └───────────────────────┘

         ┌───────────────┐                 ┌─────────────────┐
         │  cpi_period   │                 │  cpi_footnote    │
         │ period_codePK │                 │ footnote_code PK │
         │ period_abbr   │                 │ footnote_text    │
         │ period_name   │                 └──────────────────┘
         └───────────────┘
```

Table definitions and indexes are straight from the loader’s DDL: cpi_area and cpi_item define reference metadata; cpi_series keys the BLS series with seasonal/periodicity/base fields; cpi_observation stores the time series (one row per month per series). Helpful indexes exist on display_level, item_code, and observation keys to keep queries snappy.

#### Some Cool Queries

1. **Latest available CPI month (global):**

```sql
WITH latest AS (
  SELECT MAX((year::text || '-' || RIGHT(period, 2))) AS ym
  FROM cpi_observation
)
SELECT
  SUBSTRING(ym, 1, 4)::int  AS year,
  ('M' || SUBSTRING(ym, 6, 2)) AS period
FROM latest;
```

2. **Build a proper date for observations:**

BLS stores months as M01…M12. This turns (year, period) into a DATE for plotting:

```sql
SELECT
  series_id,
  make_date(year, RIGHT(period, 2)::int, 1) AS month_date,
  value
FROM cpi_observation
LIMIT 5;
```

3. **Pull a “national, monthly, unadjusted” slice (the good stuff):**

```sql
SELECT s.series_id, s.item_code, s.area_code, o.year, o.period, o.value
FROM cpi_series s
JOIN cpi_observation o USING (series_id)
WHERE s.area_code = '0000'        -- U.S. city average
  AND s.seasonal  = 'U'           -- unadjusted
  AND s.periodicity_code = 'R';   -- monthly
```

4. **Find the top contributors to the right tail in a given month:**

```sql
WITH yoy AS (
  SELECT
    s.series_id, s.item_code, i.item_name, i.display_level,
    make_date(o.year, RIGHT(o.period, 2)::int, 1) AS month_date,
    (o.value / LAG(o.value, 12) OVER (PARTITION BY s.series_id ORDER BY o.year, o.period) - 1) AS yoy
  FROM cpi_observation o
  JOIN cpi_series s USING (series_id)
  JOIN cpi_item   i ON i.item_code = s.item_code
  WHERE s.area_code = '0000' AND s.seasonal = 'U' AND s.periodicity_code = 'R'
)
SELECT item_name, display_level, yoy
FROM yoy
WHERE month_date = DATE '2025-06-01'
ORDER BY yoy DESC
LIMIT 15;
```

---

## Gift 2: `kde-cpi analyze`

The core command computes KDE-mode inflation and compares it to other metrics:

```bash
kde-cpi analyze \
  --group-by display-level \
  --source database \
  --output-dir out/publication \
  --series-lock area_code=0000 \
  --series-lock periodicity=R \
  --series-lock seasonal=U
```

This groups data by display level, filters for unadjusted U.S. averages, and saves both visualizations and metadata.
The output looks like this:

![KDE Density](../out/publication/analysis_display_level_20251027_170515/group_1/density.png)

The distribution isn’t normal—it’s skewed, fat-tailed, and often multimodal. The KDE mode captures the true peak at roughly **2.8 %**, while the mean and median sit closer to **2 %**. It’s a reminder that in asymmetric worlds, the average is often a lie.

---

## Beneath the Surface: Metadata for the Curious

Each group also comes with a JSON summary—essentially the DNA of the plot:

```json
{
  "label": "1",
  "count": 66,
  "stats": {
    "weighted_mean_percent": "2.03%",
    "weighted_median_percent": "2.14%",
    "weighted_kde_mode_percent": "2.79%",
    "weighted_skewness": -0.94,
    "weighted_kurtosis": 2.47
  },
  "examples": [
    {"item_name": "Tobacco and smoking products", "yoy_percent": "6.92%"},
    {"item_name": "Fuels and utilities", "yoy_percent": "5.83%"},
    {"item_name": "Information technology commodities", "yoy_percent": "-5.08%"}
  ]
}
```

For the five percent who care more about the data than the chart, this is the real treasure: a complete anatomy of inflation’s shape. You can trace every tail and peak to its source.

---

## The FOMC Would’ve Seen It Coming

One of the package’s most revealing features is the `panel` command, which builds a time-series of KDE-mode metrics across display levels:

```bash
kde-cpi panel \
  --group-by display-level \
  --start 2006-12 \
  --end 2009-12 \
  --series-lock area_code=0000 \
  --series-lock periodicity=R \
  --series-lock seasonal=U
```

![Panel](../out/panel.png)

During the Global Financial Crisis, the KDE mode tracked the traditional measures—but with far less noise. It would have signaled genuine disinflation without overreacting to transient commodity swings. In short, the FOMC might have avoided chasing ghosts in the oil market.

---

## Covid: The Test of Fire

Running the same analysis through the pandemic produces this:

```bash
kde-cpi metrics-timeseries \
  --start 2019-12 \
  --end 2025-12 \
  --series-lock area_code=0000 \
  --series-lock periodicity=R \
  --series-lock seasonal=U
```

![Covid Metrics](../out/metrics.png)

From 2019 through 2024, the KDE mode tracked the structural inflation pulse without being thrown by temporary shocks. It shows what most prices were actually doing—rising steadily—long after traditional measures suggested volatility. The story isn’t “mystery inflation”; it’s the mode telling us that the experience of inflation was real, just unevenly distributed.

---

## The Shape of Truth

Inflation isn’t a single number—it’s a geometry of prices. By visualizing that geometry, KDE reveals what traditional statistics flatten.

This project began as an irritation at bad reporting, but it ended as a small argument for epistemic humility. Measurement isn’t just arithmetic; it’s a philosophy. And the first rule of measurement, like economics itself, is simple: **listen to what the data is saying—before you tell it what it means.**
