# Methodology

Notation: `V` = position PV (dirty, position currency); `s` = scenario; `x_{s,i}` = shock of risk
factor `i` in scenario `s` (bp); `PnL_s = V(shocked market_s) − V(base market)`.

## 1. Curve and bond pricing

**Curve.** Continuously compounded zero rates `z(t)` at pillars `t_1..t_n` (1M…30Y), linear in
`t`, flat outside the pillar range. Time is ACT/365F from the business date.

All interpolation goes through one weight matrix `W(grid, x)` with `f(x) = Wᵀ f(grid)`
(`curves.linear_interp_weights`). The same weights shift the curve: a shock vector `x_s` on the
pillars becomes `Δz_s(t) = Wᵀ x_s` at each cash-flow time. Because `W` is linear, a shocked curve
is exactly the interpolation of the shocked pillars.

**Bond.** The schedule is generated backward from maturity (12/frequency months, end-of-month
rule taken from maturity, short first stub at the issue date). Pay dates are adjusted FOLLOWING
on the bond's holiday calendar; accrual uses the unadjusted dates. Coupons follow ACT/ACT ICMA,
so a regular period pays exactly `c/f`. A cash flow counts if its pay date is after the business date.

```
V      = Σ_j CF_j · exp(−z(t_j) · t_j)                    (dirty)
V_s    = Σ_j CF_j · exp(−(z(t_j) + Δz_s(t_j)) · t_j)       (all scenarios in one matrix product)
AI     = N · c/f · days(last coupon, business date) / days(period)
clean  = V − AI
```

## 2. Sensitivities and their conventions

| | Definition | Basis |
|---|---|---|
| Engine delta | `(V(+1bp) − V(−1bp)) / 2` per pillar | curve pillars (= scenario risk factors) |
| Engine gamma | `V(+1bp) − 2V + V(−1bp)` per bp² | curve pillars |
| FRTB GIRR delta (MAR21.19) | `(V(r_k + 0.0001) − V(r_k)) / 0.0001` | FRTB vertices 0.25Y…30Y (triangular bump on the vertex grid) |
| FO DV01 | `−(V(+1bp) − V(−1bp)) / 2` (positive = loss) | FO buckets (FO names, mapped via Reference) |
| FO gamma | diagonal key-rate gamma per bp² | FO buckets |

Canonicalisation (`frtb.SENSITIVITY_CONVENTIONS`): FRTB × 1e-4, FO DV01 × −1, FO gamma × 1. An
unknown `(source, type, unit)` combination fails data-quality checks rather than being guessed.

**Reconciliation (step 06).** The FRTB feed is compared with engine vertex deltas (same basis, same forward
bump). FO is compared with engine pillar deltas and gammas. Tolerance = max(USD 0.50 per bp,
1% of the engine value). Small differences come from rounding and from forward vs central
differences (about γ/2 per bp).

## 3. Risk-factor mapping

* **Curve pillar → scenario risk factor:** interpolation weights (identity here because the pillars coincide).
* **Position → risk factors:** the pillars with a non-zero weight for any of the position's cash-flow times.
* **Sensitivity risk factor → scenario risk factor:**
  * FRTB vertex `v` on curve `c` maps to the scenario risk factors of curve `c` with weights `W(scenario tenors, v)`. For example, a 15Y vertex would get 0.5 × 10Y + 0.5 × 20Y; 5Y maps one-to-one.
  * FO names map through the governed `rf_name_mapping` table. An unmapped name stops the run.

## 4. The four PnL methods

### 4.1 Full revaluation
`PnL_s = V_s − V`. This is the benchmark. It costs one pricing per scenario and position, done
as one vectorised NumPy product per position.

### 4.2 Taylor (sensitivity-based)
```
PnL_s ≈ Σ_k δ_k · x̃_{s,k}  +  ½ Σ_b γ_b · x̂_{s,b}²
x̃_{s,k} = Σ_i w_{k,i} x_{s,i}      (scenario shocks mapped to FRTB vertex k)
x̂_{s,b} = Σ_i w_{b,i} x_{s,i}      (scenario shocks mapped to FO bucket b)
```
It runs as a Spark join and aggregation. Known error sources:

* **Third and higher order terms:** small for bonds, large for options.
* **Cross-gammas `γ_{ij}`:** ignored with a diagonal gamma. A cash flow between two pillars has
  `∂²V/∂z_i∂z_j ≠ 0`. On a +200bp parallel shift of the 2036 bond this costs about USD 9k.
* **Basis mismatch:** FRTB has no 7Y vertex, so a scenario whose 7Y shock is not linear
  between 5Y and 10Y is mis-measured. This causes the +2.4% VaR difference on the 2036 bond.
  `tests/test_methods.py::test_vertex_basis_mapping_error_for_kinked_shock` shows it in isolation.

### 4.3 Grid
Pre-computed once: `G(u) = V(all exposed RFs shifted by u) − V` at nodes `u ∈ {−250…+250}` bp.
A scenario's coordinate is its DV01-weighted average shock:
```
u_s = Σ_i w_i x_{s,i},   w_i = δ_i / Σ_j δ_j        (equal weights if Σδ ≈ 0)
PnL_s ≈ interp(G, u_s)
```
* **First order is preserved:** `(Σδ) · u_s = Σ δ_i x_{s,i}`.
* **Parallel convexity is exact:** it comes from the revaluations.
* **Curve shape is compressed into one number.** That is visible on twist scenarios.
* **Linear interpolation adds a chord error** of about `G'' h² / 8` for node spacing `h`, which is
  why the nodes are dense near zero (5–10bp).
* **Points outside the node range are extrapolated linearly and flagged** (policy `LINEAR_FLAG`;
  `ERROR` and `FLAT_FLAG` are also available). Linear extrapolation of a convex profile overstates
  losses.

### 4.4 Ladder
Pre-computed once per risk factor `i`: `L_i(u) = V(RF i shifted by u, all others at base) − V`.
```
PnL_s ≈ Σ_i interp(L_i, x_{s,i})
```
It captures each factor's own non-linearity exactly at the nodes. It misses interactions between
factors (cross-gamma), so on large parallel moves it diverges like diagonal-gamma Taylor.

## 5. Risk measures

* **Aggregation:** vectors are summed scenario by scenario at POSITION, BOOK, DESK and PORTFOLIO
  level. VaR is computed on the summed vector and never by adding VaRs.
* **VaR (historical simulation, KTH_WORST):** the k-th largest loss with `k = ⌈N(1−α)⌉`. For
  α = 99% that is the 3rd worst of 260 (VaR) or of 250 (SVaR). `NUMPY_LINEAR` uses an
  interpolated quantile instead.
* **ES 97.5%:** the mean of the `⌈N · 2.5%⌉` worst outcomes (7 of 260).
* **10-day VaR:** √10 × 1-day, for reporting only. Overlapping 10-day shocks are a planned option.
* **Method accuracy (step 10):** mean and max absolute difference vs full revaluation, VaR
  difference, Spearman correlation and the two-sample KS statistic. Zones use the FRTB PLA
  thresholds (green when Spearman ≥ 0.80 and KS ≤ 0.09; red when Spearman < 0.70 or KS > 0.12).
  This is an analogy only: the real PLA test compares front-office hypothetical PnL with
  risk-theoretical PnL over 250 days.
