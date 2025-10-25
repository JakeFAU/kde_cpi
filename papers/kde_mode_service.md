# Kernel Density Estimation for CPI Analytics

*A Practical and Statistical Rationale for KDE-Based Inflation Measurement*

---

## 1. Motivation

Consumer Price Index (CPI) data contain tens of thousands of individual price relatives, each representing how the price of a particular good or service evolves across time and geography. Traditional summary statistics—such as the mean, median, or trimmed mean—compress this complexity into a single value. While convenient, these summaries obscure important features of the underlying distribution, especially when price changes are fat-tailed or multi-modal.

The **`kde-cpi`** service instead estimates the *entire distribution* of component-level inflation rates and identifies its **mode**, the most probable inflation rate. This produces a measure that reflects the dominant price-change behavior rather than being skewed by extremes.

```bash
# Build a fresh CPI dataset snapshot
kde-cpi ingest --data-file cu.data.0.Current --schema cpi_app

# Compute weighted KDE statistics for the latest period
kde-cpi stats --schema cpi_app --group-by area_code --output out/stats.json
```

---

## 2. Kernel Density Estimation Primer

Given weighted observations
$$
\{(x_i, w_i)\}_{i=1}^n \quad \text{with} \quad \sum_i w_i = 1,
$$

the Gaussian KDE is:

$$
\hat{f}_h(x) = \frac{1}{h} \sum_{i=1}^{n} w_i \, K\!\left(\frac{x - x_i}{h}\right),
\qquad
K(u) = \frac{1}{\sqrt{2\pi}} \exp\!\left(-\frac{u^2}{2}\right).
$$

Here \( h > 0 \) is the bandwidth.

Weighted Scott’s rule (with effective sample size \( n_{\text{eff}} = 1 / \sum_i w_i^2 \)):

$$
h = 0.9 \,\min\!\left(\sigma, \frac{\mathrm{IQR}}{1.34}\right)\, n_{\text{eff}}^{-1/5}.
$$

The KDE mode is:

$$
x^\star = \arg\max_x \, \hat{f}_h(x).
$$

---

## 3. Comparing Measures of Central Tendency

| Statistic        | Formula (weighted)                            | Strengths                                     | Limitations                      |
| ---------------- | --------------------------------------------- | --------------------------------------------- | -------------------------------- |
| **Mean**         | (\mu = \sum_i w_i x_i)                        | Simple, additive                              | Sensitive to outliers            |
| **Median**       | Smallest (m) with cumulative weight (\ge 0.5) | Robust to extremes                            | Ignores multi-modality           |
| **Trimmed Mean** | Mean after clipping tails                     | Reduces tail effects                          | Requires arbitrary trim fraction |
| **KDE Mode**     | (x^* = \arg\max_x \hat{f}_h(x))               | Captures dominant behavior; multi-modal aware | Requires bandwidth tuning        |

In inflation analysis, extreme swings in a few sectors (like energy or vehicles) can distort the mean or even the median. The KDE mode filters these out by focusing on the densest region of the weighted distribution—the inflation rate at which most components cluster.

---

## 4. Weighted Statistical Pipeline

The service computes a consistent suite of statistics:

1. **Weighted moments** — mean, variance, skewness, and kurtosis computed from normalized weights.
2. **Trimmed mean** — calculated using cumulative-weight clipping to reduce tail influence.
3. **Effective sample size** — *n_eff = 1 / Σ w_i^2*, which gauges statistical reliability.
4. **KDE bandwidth** — estimated from Scott’s rule using the weighted standard deviation and interquartile range.
5. **KDE mode** — located by evaluating
$$
\hat{f}_h(x)
$$
over a grid (default: 2048 points).

```bash
# Generate chart-ready KDE plots and histograms
kde-cpi plots generate --schema cpi_app --output-dir out/plots

# Export JSON summary (mean, median, KDE mode, ESS)
kde-cpi stats --schema cpi_app --group-by item_code --output out/item_stats.json
```

---

## 5. Visualizing Why KDE Mode Works

### Example: Skewed CPI Distribution

Imagine a CPI month where most categories cluster around **3% inflation**, but a few volatile items jump **30%**.

Below is an illustration:

```
            Density Comparison
            ^
Density     |
   |                   *            KDE Mode (≈3%)
   |                *
   |             *
   |         *
   |     *           Mean (≈4.5%)
   | *                Median (≈3.3%)
   +---------------------------------->
            Inflation Rate (%)
```

* The **mean** is pulled rightward by the long tail of outliers.
* The **median** resists some distortion but ignores that the bulk is sharply peaked.
* The **KDE mode** lands squarely on the modal cluster, providing a stable, interpretable signal.

KDE mode thus tells us *where the mass of inflation lies*, not merely its arithmetic average.

---

## 6. Why KDE Mode Excels for Inflation Analytics

1. **Non-parametric flexibility**: CPI data are far from Gaussian. KDE adapts naturally to skewness, fat tails, and multiple peaks without assuming any distributional form.
2. **Tail de-emphasis**: Gaussian kernels decay exponentially, so extreme values contribute negligibly to the density maximum.
3. **Interpretability**: The KDE mode answers the intuitive question: *At what rate are most prices rising?*
4. **Smoothness**: KDE generates continuous curves that yield stable time series of modal inflation.
5. **Weight sensitivity**: Each component enters the density with its CPI expenditure share, preserving the true weighting structure.

---

## 7. From Raw Data to Insight

1. **Ingestion**: `kde-cpi ingest` downloads and parses BLS flat files, normalizing codes for areas, items, and periods.
2. **Transformation**: Observations are deduplicated, weights normalized, and metadata preserved.
3. **Computation**: Commands like `stats` and `plots` query the PostgreSQL store, convert observations to arrays, and compute the statistics above.
4. **Presentation**: The results include JSON summaries and annotated plots highlighting the mean, median, and KDE mode.

```bash
# End-to-end workflow example
kde-cpi ingest --data-file cu.data.0.Current --schema cpi_app
kde-cpi stats --schema cpi_app --group-by area_code --output out/area_stats.json
kde-cpi plots generate --schema cpi_app --output-dir out/plots
```

---

## 8. Mathematical Intuition & Broader Context

KDE mode estimation is a non-parametric way of finding the **maximum likelihood point** of the unknown true density. If we think of each CPI component as a random draw from some hidden inflation-generating process, the KDE smooths those draws to reveal the process’s shape.

* The **bandwidth** (h) controls the bias–variance tradeoff: small (h) captures noise, large (h) hides structure.  Scott’s rule balances these forces automatically.
* The **mode** (x^*) is the density peak—the most probable inflation rate.  As sample size grows, (x^*) converges to the population mode under standard regularity conditions.
* The **effective sample size** ensures that confidence in the mode reflects the diversity of weighted contributions.

In practice, this gives analysts a **statistically consistent** and **economically intuitive** view of inflation dynamics: the KDE mode tracks the typical rate of price change across the economy’s most representative components.

---

## 9. Conclusion

The `kde-cpi` framework fuses rigorous non-parametric statistics with reproducible, production-grade tooling for inflation analysis. By emphasizing the **mode** of the CPI component distribution rather than the mean, analysts gain a signal that is:

* **Statistically robust**, resisting tail-driven distortion.
* **Economically meaningful**, showing the inflation most prices actually experience.
* **Operationally transparent**, built from open data and reproducible commands.

As inflation analysis evolves beyond point estimates, modal measures like KDE mode offer a sharper lens—revealing the true texture of price change distributions rather than their arithmetic shadows.
