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

## KDE Mode

So here’s the alternative: stop pretending we know the shape of inflation. Let the data tell us.

A **Kernel Density Estimate (KDE)** is a nonparametric way of estimating a probability distribution. Instead of assuming a bell curve, it builds a landscape—placing a small “bump” of probability around each data point, then summing them all. The result is a smooth estimate of how prices actually move, revealing asymmetries, fat tails, and multiple peaks that traditional models hide.

Where the mean and median reduce an economy to a single number, KDE lets us see the terrain—the ridges and valleys of price change across categories. The **mode** of that landscape—the point of maximum density—captures the most common rate of inflation in the economy at that moment. It’s robust to outliers, immune to the arbitrary weightings that dominate CPI, and less prone to distortion by rent-heavy categories. It tells you what *most prices* are doing, not just what the biggest or noisiest ones are.

Rather than trimming or excluding, we listen. The KDE mode finds the “consensus” inflation rate implied by the data itself—not by the assumptions of 1970s econometricians.

---

## `kde_mode`: Turning the Idea into a Tool

`kde_mode` is a Python package that fetches CPI data directly from the BLS, computes the KDE of component price changes, and extracts the mode.
It’s open source, available on [GitHub](https://github.com/JakeFAU/kde_cpi), and installable via pip:

```bash
pip install kde_mode
```

It runs from the command line or as an importable library. Documentation is [here](https://jakefau.github.io/kde_cpi/), but it’s designed to be intuitive—data in, distribution out.

---

## Gift 1: A Modern Database from 1950s Files

This is the first gift `kde_mode` gives you: a full modernization of the BLS’s creaky infrastructure.
The BLS still distributes CPI data as dozens of flat files—a format better suited for a mainframe than modern analytics. `kde_mode` automates the entire ingestion process: downloading, decoding, and stitching every partition into a relational **PostgreSQL** database. The result is clean, normalized, and query-ready for any analytical or machine learning workflow.

There’s even a `docker-compose.yml` that builds the database and launches **pgAdmin** for browsing. You can point it at your own DSN if you prefer. It’s a small quality-of-life revolution for anyone who’s ever parsed BLS flat files by hand.

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
