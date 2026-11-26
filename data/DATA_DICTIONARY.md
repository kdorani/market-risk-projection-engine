# Sample data dictionary

All files are **synthetic**, generated deterministically by `scripts/generate_sample_data.py`
(seed 20261124). Business date: **2026-11-24**. Schemas and primary keys are enforced by
`src/projection_engine/schemas.py`.

## Position data (`data/source/position_data/`)

| File | Rows | Key | Content |
|---|---|---|---|
| `positions.csv` | 3 | business_date, position_id | One row per trade: book, desk, product type, instrument, currency, **signed notional** (+ long / − short) |
| `bond_terms.csv` | 3 | business_date, instrument_id | Static bond terms: issue and maturity dates, coupon, frequency, day count, business-day convention, calendar, discount curve |

Positions:

| position | instrument | notional | coupon | freq | maturity | book |
|---|---|---|---|---|---|---|
| POS-0001 | DEMO-BOND-6.00-2031 | +10,000,000 | 6.00% | annual | 2031-11-15 (Sat, paid Mon 17th) | RATES_BOOK_A |
| POS-0002 | DEMO-BOND-3.50-2028 | −20,000,000 | 3.50% | semi-annual | 2028-11-15 | RATES_BOOK_B |
| POS-0003 | DEMO-BOND-4.25-2036 | +5,000,000 | 4.25% | semi-annual | 2036-08-15 | RATES_BOOK_A |

## Sensitivities data (`data/source/sensitivities_data/`)

| File | Rows | Key | Content |
|---|---|---|---|
| `frtb_sensitivities.csv` | 16 | business_date, position_id, risk_class, risk_type, bucket, risk_factor, vertex | FRTB SA **GIRR delta** per FRTB vertex (MAR21.8 vertices; MAR21.19 definition: forward 1bp bump ÷ 0.0001, so **USD per unit of rate**). Only non-zero vertices are sent |
| `fo_sensitivities.csv` | 36 | business_date, position_id, risk_factor_name, sensitivity_type | Front-office **DV01** (USD/bp, positive = loss) and **GAMMA** (USD/bp²) per curve bucket, under FO risk-factor names such as `USD.TSY.ZERO.5Y`. **POS-0002 / 1Y DV01 is deliberately 3% stale** so the reconciliation has a break to find |

## Shocks data (`data/source/shocks_data/`)

| File | Rows | Key | Content |
|---|---|---|---|
| `risk_factors.csv` | 11 | rf_id | Scenario risk factors `IR.USD.TSY.ZERO.{1M…30Y}` with shock type (ABSOLUTE) and unit (BP) |
| `scenario_sets.csv` | 3 | business_date, scenario_set | Set metadata: window, horizon, number of scenarios, data origin |
| `scenario_shocks.csv` | 5,742 | business_date, scenario_set, scenario_id, rf_id | Long format, one row per scenario × risk factor (522 × 11) |

The scenario sets:

* **VAR_1D:** 260 daily shocks dated 2025-11-10 to 2026-11-24. Level/slope/curvature factor
  model with Student-t tails; daily volatility about 5bp.
* **SVAR_1D:** 250 daily shocks dated 2022-03-01 onward. Volatility about 8–9bp, a small upward
  drift and three CPI-day style jumps (+28, +22, −30bp level). The dates are labels only: the
  shocks are synthetic, not real 2022 data. Use `scripts/convert_treasury_csv.py` to load real
  history.
* **STRESS_HYPO:** 12 hypothetical shocks:
  * parallel ±10/25/15/30 (the four shocks of release 0.1), ±100 and +200bp
  * a +300bp shock beyond the grid on purpose
  * bear steepener, bull flattener, a 2s10s twist and a belly hump

## Reference data (`data/source/reference/`)

| File | Rows | Key | Content |
|---|---|---|---|
| `yield_curves.csv` | 11 | business_date, curve_id, tenor_label | `USD_TSY` continuously compounded zero curve, linear on zero rates |
| `holiday_calendars.csv` | 198 | calendar_id, holiday_date | `USNY`: US federal holidays 2020–2037, weekend-observed |
| `rf_name_mapping.csv` | 11 | source_system, source_risk_factor | Governed mapping from FO risk-factor names to engine `rf_id` |

## Lineage columns added at ingestion

`_source_system`, `_source_object`, `_load_ts`, plus `run_id` on every warehouse table.
`run_metadata.json` records the SHA-256 of every source file.
