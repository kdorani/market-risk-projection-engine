# Market Risk Projection Engine (PySpark)

A small, extensible demonstration of a bank-style market-risk P&L projection engine.
The first release computes scenario P&L for one **USD fixed-coupon bond** using:
1. Full revaluation
2. Sensitivity-based Taylor expansion (delta + gamma)
3. Grid-based valuation with linear interpolation
4. Ladder-based key-rate sensitivity approximation

> **Data disclaimer:** All records in `data/source/` are synthetic, illustrative data created for this public portfolio. They are not real data and do not represent actual systems, schemas, models, market data, or methodologies.

## Scope and assumptions

- Business date: `2026-11-24`
- Instrument: one USD fixed-coupon bond, face value USD 10,000,000, coupon 6%, annual, five years remaining.
- Valuation: annual cash flows discounted using a flat continuously compounded zero rate for this teaching example.
- Base zero rate: 5.00%.
- Scenario shocks: parallel shifts in basis points applied to the flat curve.
- P&L sign: scenario PV minus base PV.
- Grid: precomputed at selected flat rates, with linear interpolation.
- Ladder: key-rate DV01 buckets. For the initial bond, the bucket sensitivities sum to the model's parallel DV01. The scenario shock at each bucket is applied to its bucket sensitivity.
- FRTB sensitivities are the risk-model sensitivity feed. FO sensitivities are retained as a separate source for reconciliation. The example uses the DV01 / convexity for Taylor and key-rate DV01 for ladder.
- The sample is deliberately small. Production implementations need approved pricing libraries, curve construction, calendars, day-count conventions, compounding conventions, accrued interest, coupon schedules, business-day adjustments, curve interpolation, market-data quality controls, scenario governance, and independent model validation.

## Architecture

```text
data/source/{position source, shock source, sensitivity source, reference source}  (CSV source extracts)
                    |
             PySpark ingestion
                    |
       data/warehouse/<table>/*.parquet
                    |
       risk-factor mapping + scenario expansion
                    |
         four P&L calculation methodologies
                    |
        data/output/pnl_vectors/*.parquet
        data/output/pnl_summary/*.csv
```

## Requirements

- Python 3.10 or 3.11
- Java 11 or 17 (required by Spark)
- PySpark 3.5.x
- Pytest

## Setup in PyCharm

1. Open this folder as a PyCharm project.
2. Configure a Python 3.10/3.11 virtual environment.
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Install/configure Java and set `JAVA_HOME` if Spark cannot find Java.
5. Run the engine from the project root:

   ```bash
   python -m src.projection_engine.main
   ```

   The first command is the recommended local PyCharm run configuration. Set the working directory to the project root.

6. Run tests:

   ```bash
   pytest -q
   ```

## Expected outputs

The application writes:
- `data/warehouse/` — internal Parquet tables loaded from source CSV extracts.
- `data/output/pnl_vectors/` — long-form P&L vectors, one row per position/scenario/method.
- `data/output/pnl_summary/` — CSV comparison summary.

A P&L vector is stored in long format rather than as a single array column. This is easier to query, partition, aggregate and validate in Spark. A vector for a trade is the ordered collection of its scenario P&Ls.

## Source table inventory

| Source | File | Purpose |
|---|---|---|
| sensitivities_data | `frtb_sensitivities.csv` | Model sensitivities: parallel DV01, convexity, key-rate DV01 |
| shocks_data | `scenario_shocks.csv` | Scenario shocks by risk-factor key |
| position_data | `positions.csv` | Trade/position attributes |
| sensitivities_data | `fo_sensitivities.csv` | Front-office sensitivity snapshot, separate from FRTB sensitivity |
| Reference | `yield_curve_nodes.csv` | Base curve nodes |
| Reference | `pricing_grid.csv` | Precomputed price grid for this instrument |

## Methodology notes

### Full revaluation
Rebuilds the shocked curve and discounts all contractual cash flows. It is the reference calculation in this example.

### Taylor expansion
Uses first- and second-order sensitivities:
`dV ~= -DV01 * shock_bp + 0.5 * convexity * PV * shock_decimal^2`.
Here `convexity` is defined consistently with the conventional bond price-yield convexity formula and the rate movement is in decimal units.

### Grid-based
Looks up precomputed prices at grid nodes and linearly interpolates between adjacent nodes. The example grid is one-dimensional (flat rate). A real option grid is commonly multi-dimensional (e.g. underlying, volatility, expiry, tenor).

### Ladder-based
Uses key-rate DV01 buckets:
`dV ~= -sum_i(KRDV01_i * shock_i_bp)`.
This example uses seven tenor buckets and shocks. It is a first-order bucketed approximation, not full revaluation. Ladder-based is a risk-factor representation/calculation approach and can coexist with Taylor or full revaluation.

## Important implementation caveats

This is a learning project, not a production risk engine:
- The sample curve is flat and continuously compounded to keep the pricing calculation transparent.
- All four methods use the same four parallel scenarios in v1, so their outputs are comparable. Ladder still applies the shock bucket-by-bucket using key-rate DV01. Non-parallel tenor shocks are a planned extension.
- The fixed-coupon bond cash flows are annual and undiscounted principal is repaid at maturity.
- The example uses one trade and five scenarios; it does not demonstrate distributed-scale performance.
- No VaR/SVaR percentile calculation is included yet. The output is designed to be VaR-ready.
- No real database credentials, connectors, or confidential source schemas are included.

## Suggested next enhancements

1. Add floating-rate bonds and reset schedules.
2. Add swaps with discount/projection curves.
3. Add FX forwards and equity positions.
4. Add options and multi-dimensional grids.
5. Add scenario-specific non-parallel curve shocks to full revaluation.
6. Add partitioning, incremental loads, data-quality metrics and run metadata.
7. Add a VaR/ES consumer and backtesting module.
