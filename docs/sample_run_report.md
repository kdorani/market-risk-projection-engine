# Projection engine run report

- run_id: `2026-11-24_160249_3745e3`
- business date: 2026-11-24
- methods: FULL_REVALUATION, TAYLOR, GRID, LADDER
- scenario sets: {'STRESS_HYPO': 12, 'SVAR_1D': 250, 'VAR_1D': 260}
- engine 0.2.0, PySpark 4.3.0, Python 3.13.18

## Step 01: Ingestion (position, sensitivity, shock and reference sources -> engine staging tables)

Each upstream table is read with an explicit schema, filtered to the business date, tagged with lineage columns and persisted as an engine-owned Parquet table (stg_*).

**ingestion_summary** — 10 rows (`01_ingestion_summary.csv`)

| source_system | source_object | staging_table | rows_read | rows_for_business_date | columns | primary_key | sha256_prefix |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POSITION_DATA | positions | stg_positions | 3 | 3 | 11 | business_date, position_id | e8265ecceef9 |
| POSITION_DATA | bond_terms | stg_bond_terms | 3 | 3 | 13 | business_date, instrument_id | 5980ad3d0b0c |
| SENSITIVITIES_DATA | fo_sensitivities | stg_fo_sensitivities | 36 | 36 | 9 | business_date, position_id, risk_factor_name, sensitivity_type | 479a9667e4d4 |
| SENSITIVITIES_DATA | frtb_sensitivities | stg_frtb_sensitivities | 16 | 16 | 13 | business_date, position_id, risk_class, risk_type, bucket, risk_factor, vertex | 1a0f0aa3ac91 |
| SHOCKS_DATA | risk_factors | stg_risk_factors | 11 | 11 | 10 | rf_id | cb9831f381f1 |
| SHOCKS_DATA | scenario_sets | stg_scenario_sets | 3 | 3 | 8 | business_date, scenario_set | 76d2ee157110 |
| SHOCKS_DATA | scenario_shocks | stg_scenario_shocks | 5742 | 5742 | 8 | business_date, scenario_set, scenario_id, rf_id | 32fce00009d1 |
| REFERENCE | yield_curves | stg_yield_curves | 11 | 11 | 9 | business_date, curve_id, tenor_label | 824d79956fb2 |
| REFERENCE | holiday_calendars | stg_holiday_calendars | 198 | 198 | 3 | calendar_id, holiday_date | 4e5f3789c510 |
| REFERENCE | rf_name_mapping | stg_rf_name_mapping | 11 | 11 | 5 | source_system, source_risk_factor | 6c9a6633200f |

**stg_positions** — 3 rows (`01_stg_positions.csv`)

| position_id | trade_id | book_id | desk_id | product_type | instrument_id | currency | notional | trade_date | source_system |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | TRD-BOND-0001 | RATES_BOOK_A | USD_RATES | FIXED_RATE_BOND | DEMO-BOND-6.00-2031 | USD | 10,000,000.00 | 2024-03-12 | POSITION_DATA |
| POS-0002 | TRD-BOND-0002 | RATES_BOOK_B | USD_RATES | FIXED_RATE_BOND | DEMO-BOND-3.50-2028 | USD | -20,000,000.00 | 2026-02-03 | POSITION_DATA |
| POS-0003 | TRD-BOND-0003 | RATES_BOOK_A | USD_RATES | FIXED_RATE_BOND | DEMO-BOND-4.25-2036 | USD | 5,000,000.00 | 2026-09-01 | POSITION_DATA |

**stg_bond_terms** — 3 rows (`01_stg_bond_terms.csv`)

| instrument_id | isin | issuer | currency | issue_date | maturity_date | coupon_rate | coupon_frequency | day_count | business_day_convention | calendar_id | discount_curve_id |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DEMO-BOND-6.00-2031 | DEMO00000001 | DEMO_SOVEREIGN | USD | 2021-11-15 | 2031-11-15 | 0.06 | 1 | ACT/ACT_ICMA | FOLLOWING | USNY | USD_TSY |
| DEMO-BOND-3.50-2028 | DEMO00000002 | DEMO_SOVEREIGN | USD | 2025-11-15 | 2028-11-15 | 0.035 | 2 | ACT/ACT_ICMA | FOLLOWING | USNY | USD_TSY |
| DEMO-BOND-4.25-2036 | DEMO00000003 | DEMO_SOVEREIGN | USD | 2026-08-15 | 2036-08-15 | 0.0425 | 2 | ACT/ACT_ICMA | FOLLOWING | USNY | USD_TSY |


## Step 02: Data-quality checks

Keys, referential integrity, ranges, units and scenario-matrix completeness. Any ERROR check that fails stops the run before pricing.

**dq_results** — 46 rows (`02_dq_results.csv`)

| table | check | severity | status | failed_rows | sample |
| --- | --- | --- | --- | --- | --- |
| positions | primary_key_not_null | ERROR | PASS | 0 |  |
| positions | primary_key_unique | ERROR | PASS | 0 |  |
| bond_terms | primary_key_not_null | ERROR | PASS | 0 |  |
| bond_terms | primary_key_unique | ERROR | PASS | 0 |  |
| fo_sensitivities | primary_key_not_null | ERROR | PASS | 0 |  |
| fo_sensitivities | primary_key_unique | ERROR | PASS | 0 |  |
| frtb_sensitivities | primary_key_not_null | ERROR | PASS | 0 |  |
| frtb_sensitivities | primary_key_unique | ERROR | PASS | 0 |  |
| risk_factors | primary_key_not_null | ERROR | PASS | 0 |  |
| risk_factors | primary_key_unique | ERROR | PASS | 0 |  |
| scenario_sets | primary_key_not_null | ERROR | PASS | 0 |  |
| scenario_sets | primary_key_unique | ERROR | PASS | 0 |  |
| scenario_shocks | primary_key_not_null | ERROR | PASS | 0 |  |
| scenario_shocks | primary_key_unique | ERROR | PASS | 0 |  |
| yield_curves | primary_key_not_null | ERROR | PASS | 0 |  |
| yield_curves | primary_key_unique | ERROR | PASS | 0 |  |
| holiday_calendars | primary_key_not_null | ERROR | PASS | 0 |  |
| holiday_calendars | primary_key_unique | ERROR | PASS | 0 |  |
| rf_name_mapping | primary_key_not_null | ERROR | PASS | 0 |  |
| rf_name_mapping | primary_key_unique | ERROR | PASS | 0 |  |
| positions | product_type_supported | ERROR | PASS | 0 |  |
| positions | notional_non_zero | ERROR | PASS | 0 |  |
| positions | bond_has_terms | ERROR | PASS | 0 |  |
| positions | currency_matches_terms | ERROR | PASS | 0 |  |
| bond_terms | maturity_after_issue | ERROR | PASS | 0 |  |
| bond_terms | not_matured | ERROR | PASS | 0 |  |
| bond_terms | coupon_frequency_valid | ERROR | PASS | 0 |  |
| bond_terms | coupon_rate_in_range | WARN | PASS | 0 |  |
| bond_terms | day_count_supported | ERROR | PASS | 0 |  |
| bond_terms | discount_curve_exists | ERROR | PASS | 0 |  |
| bond_terms | calendar_exists | ERROR | PASS | 0 |  |
| yield_curves | zero_rate_in_range | ERROR | PASS | 0 |  |
| yield_curves | tenor_positive | ERROR | PASS | 0 |  |
| risk_factors | shock_type_unit_known | ERROR | PASS | 0 |  |
| scenario_shocks | rf_defined_in_risk_factors | ERROR | PASS | 0 |  |
| scenario_shocks | unit_matches_rf_definition | ERROR | PASS | 0 |  |
| scenario_shocks | abs_shock_below_1000bp | WARN | PASS | 0 |  |
| scenario_shocks | scenario_matrix_complete | ERROR | PASS | 0 |  |
| scenario_shocks | configured_sets_present | ERROR | PASS | 0 |  |
| scenario_sets | n_scenarios_matches_shocks | ERROR | PASS | 0 |  |
| frtb_sensitivities | unit_convention_known | ERROR | PASS | 0 |  |
| frtb_sensitivities | girr_vertex_valid | ERROR | PASS | 0 |  |
| frtb_sensitivities | position_exists | ERROR | PASS | 0 |  |
| fo_sensitivities | unit_convention_known | ERROR | PASS | 0 |  |
| fo_sensitivities | risk_factor_name_mapped | ERROR | PASS | 0 |  |
| positions | has_frtb_delta_for_taylor | ERROR | PASS | 0 |  |


## Step 03: Market data and base valuation

Base zero curve (with its node -> scenario risk-factor mapping), bond schedules with business-day-adjusted pay dates and ACT/ACT ICMA accruals, and base PVs (dirty = PV of future cash flows; clean = dirty - accrued).

**base_curve_and_node_mapping** — 11 rows (`03_base_curve_and_node_mapping.csv`)

| curve_id | node_label | node_tenor_years | zero_rate | rf_id | weight |
| --- | --- | --- | --- | --- | --- |
| USD_TSY | 1M | 0.0833 | 0.0395 | IR.USD.TSY.ZERO.1M | 1 |
| USD_TSY | 3M | 0.25 | 0.0392 | IR.USD.TSY.ZERO.3M | 1 |
| USD_TSY | 6M | 0.5 | 0.0385 | IR.USD.TSY.ZERO.6M | 1 |
| USD_TSY | 1Y | 1 | 0.0372 | IR.USD.TSY.ZERO.1Y | 1 |
| USD_TSY | 2Y | 2 | 0.0358 | IR.USD.TSY.ZERO.2Y | 1 |
| USD_TSY | 3Y | 3 | 0.0355 | IR.USD.TSY.ZERO.3Y | 1 |
| USD_TSY | 5Y | 5 | 0.0362 | IR.USD.TSY.ZERO.5Y | 1 |
| USD_TSY | 7Y | 7 | 0.0376 | IR.USD.TSY.ZERO.7Y | 1 |
| USD_TSY | 10Y | 10 | 0.0395 | IR.USD.TSY.ZERO.10Y | 1 |
| USD_TSY | 20Y | 20 | 0.044 | IR.USD.TSY.ZERO.20Y | 1 |
| USD_TSY | 30Y | 30 | 0.0448 | IR.USD.TSY.ZERO.30Y | 1 |

**position_valuation** — 3 rows (`03_position_valuation.csv`)

| position_id | product_type | status | message | base_pv | dirty_pv | accrued_interest | clean_pv | clean_price_per_100 | dirty_price_per_100 | macaulay_duration_years | n_future_cashflows | final_maturity_years |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | FIXED_RATE_BOND | OK |  | 11,048,127.59 | 11,048,127.59 | 14,794.52 | 11,033,333.07 | 110.33 | 110.48 | 4.4763 | 5 | 4.9836 |
| POS-0002 | FIXED_RATE_BOND | OK |  | -19,970,380.86 | -19,970,380.86 | -17,403.31 | -19,952,977.54 | 99.76 | 99.85 | 1.9269 | 4 | 1.9781 |
| POS-0003 | FIXED_RATE_BOND | OK |  | 5,183,779.25 | 5,183,779.25 | 58,322.01 | 5,125,457.24 | 102.51 | 103.68 | 7.9951 | 20 | 9.7315 |

**bond_cashflows** — 29 rows (`03_bond_cashflows.csv`)

| position_id | period_start | period_end | pay_date | accrual_factor | coupon | principal | amount | t_years | zero_rate | discount_factor | pv |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | 2026-11-15 | 2027-11-15 | 2027-11-15 | 1 | 600,000.00 | 0 | 600,000.00 | 0.9753 | 0.037264 | 0.964307 | 578,584.37 |
| POS-0001 | 2027-11-15 | 2028-11-15 | 2028-11-15 | 1 | 600,000.00 | 0 | 600,000.00 | 1.9781 | 0.035831 | 0.931577 | 558,946.42 |
| POS-0001 | 2028-11-15 | 2029-11-15 | 2029-11-15 | 1 | 600,000.00 | 0 | 600,000.00 | 2.9781 | 0.035507 | 0.899657 | 539,794.31 |
| POS-0001 | 2029-11-15 | 2030-11-15 | 2030-11-15 | 1 | 600,000.00 | 0 | 600,000.00 | 3.9781 | 0.035842 | 0.867115 | 520,268.97 |
| POS-0001 | 2030-11-15 | 2031-11-15 | 2031-11-17 | 1 | 600,000.00 | 10,000,000.00 | 10,600,000.00 | 4.9836 | 0.036194 | 0.834956 | 8,850,533.52 |
| POS-0002 | 2026-11-15 | 2027-05-15 | 2027-05-17 | 0.5 | -350,000.00 | 0 | -350,000.00 | 0.4767 | 0.038565 | 0.981783 | -343,624.21 |
| POS-0002 | 2027-05-15 | 2027-11-15 | 2027-11-15 | 0.5 | -350,000.00 | 0 | -350,000.00 | 0.9753 | 0.037264 | 0.964307 | -337,507.55 |
| POS-0002 | 2027-11-15 | 2028-05-15 | 2028-05-15 | 0.5 | -350,000.00 | 0 | -350,000.00 | 1.474 | 0.036536 | 0.947571 | -331,649.75 |
| POS-0002 | 2028-05-15 | 2028-11-15 | 2028-11-15 | 0.5 | -350,000.00 | -20,000,000.00 | -20,350,000.00 | 1.9781 | 0.035831 | 0.931577 | -18,957,599.35 |
| POS-0003 | 2026-08-15 | 2027-02-15 | 2027-02-16 | 0.5 | 106,250.00 | 0 | 106,250.00 | 0.2301 | 0.039236 | 0.991011 | 105,294.92 |
| POS-0003 | 2027-02-15 | 2027-08-15 | 2027-08-16 | 0.5 | 106,250.00 | 0 | 106,250.00 | 0.726 | 0.037912 | 0.97285 | 103,365.31 |
| POS-0003 | 2027-08-15 | 2028-02-15 | 2028-02-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 1.2274 | 0.036882 | 0.955741 | 101,547.47 |
| POS-0003 | 2028-02-15 | 2028-08-15 | 2028-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 1.726 | 0.036184 | 0.939456 | 99,817.25 |
| POS-0003 | 2028-08-15 | 2029-02-15 | 2029-02-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 2.2301 | 0.035731 | 0.923407 | 98,112.02 |
| POS-0003 | 2029-02-15 | 2029-08-15 | 2029-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 2.726 | 0.035582 | 0.907558 | 96,428.02 |
| POS-0003 | 2029-08-15 | 2030-02-15 | 2030-02-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 3.2301 | 0.035581 | 0.891429 | 94,714.28 |
| POS-0003 | 2030-02-15 | 2030-08-15 | 2030-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 3.726 | 0.035754 | 0.875272 | 92,997.63 |
| POS-0003 | 2030-08-15 | 2031-02-15 | 2031-02-18 | 0.5 | 106,250.00 | 0 | 106,250.00 | 4.2384 | 0.035933 | 0.858732 | 91,240.25 |
| POS-0003 | 2031-02-15 | 2031-08-15 | 2031-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 4.726 | 0.036104 | 0.843134 | 89,583.02 |
| POS-0003 | 2031-08-15 | 2032-02-15 | 2032-02-17 | 0.5 | 106,250.00 | 0 | 106,250.00 | 5.2356 | 0.036365 | 0.826634 | 87,829.90 |
| POS-0003 | 2032-02-15 | 2032-08-15 | 2032-08-16 | 0.5 | 106,250.00 | 0 | 106,250.00 | 5.7315 | 0.036712 | 0.810248 | 86,088.81 |
| POS-0003 | 2032-08-15 | 2033-02-15 | 2033-02-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 6.2329 | 0.037063 | 0.793732 | 84,334.04 |
| POS-0003 | 2033-02-15 | 2033-08-15 | 2033-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 6.7288 | 0.03741 | 0.777459 | 82,605.04 |
| POS-0003 | 2033-08-15 | 2034-02-15 | 2034-02-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 7.2329 | 0.037747 | 0.761075 | 80,864.25 |
| POS-0003 | 2034-02-15 | 2034-08-15 | 2034-08-15 | 0.5 | 106,250.00 | 0 | 106,250.00 | 7.7288 | 0.038062 | 0.745151 | 79,172.26 |

_showing first 25 of 29 rows_


## Step 04: Risk-factor mapping

Position -> scenario risk factors it is exposed to (engine bump-and-revalue delta and gamma per factor), and sensitivity risk factors (FRTB vertices, FO bucket names) -> scenario risk factors with interpolation weights.

**map_position_rf** — 18 rows (`04_map_position_rf.csv`)

| position_id | rf_id | delta_per_bp | gamma_per_bp2 |
| --- | --- | --- | --- |
| POS-0001 | IR.USD.TSY.ZERO.6M | -2.78 | 0 |
| POS-0001 | IR.USD.TSY.ZERO.1Y | -56.07 | 0.005 |
| POS-0001 | IR.USD.TSY.ZERO.2Y | -111.66 | 0.0209 |
| POS-0001 | IR.USD.TSY.ZERO.3Y | -299.24 | 0.0674 |
| POS-0001 | IR.USD.TSY.ZERO.5Y | -4,475.68 | 2.1818 |
| POS-0002 | IR.USD.TSY.ZERO.3M | 1.53 | -0 |
| POS-0002 | IR.USD.TSY.ZERO.6M | 16.48 | -0.0006 |
| POS-0002 | IR.USD.TSY.ZERO.1Y | 139.20 | -0.0053 |
| POS-0002 | IR.USD.TSY.ZERO.2Y | 3,690.95 | -0.7112 |
| POS-0003 | IR.USD.TSY.ZERO.1M | -0.29 | 0 |
| POS-0003 | IR.USD.TSY.ZERO.3M | -2.13 | 0 |
| POS-0003 | IR.USD.TSY.ZERO.6M | -4.11 | 0.0002 |
| POS-0003 | IR.USD.TSY.ZERO.1Y | -17.74 | 0.0012 |
| POS-0003 | IR.USD.TSY.ZERO.2Y | -39.39 | 0.0051 |
| POS-0003 | IR.USD.TSY.ZERO.3Y | -93.79 | 0.0198 |
| POS-0003 | IR.USD.TSY.ZERO.5Y | -176.14 | 0.0586 |
| POS-0003 | IR.USD.TSY.ZERO.7Y | -590.58 | 0.1667 |
| POS-0003 | IR.USD.TSY.ZERO.10Y | -3,220.28 | 2.8001 |

**map_sensitivity_rf** — 16 rows (`04_map_sensitivity_rf.csv`)

| sens_source | sens_rf_key | rf_id | weight | mapping_rule |
| --- | --- | --- | --- | --- |
| FO | USD.TSY.ZERO.10Y | IR.USD.TSY.ZERO.10Y | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.1M | IR.USD.TSY.ZERO.1M | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.1Y | IR.USD.TSY.ZERO.1Y | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.2Y | IR.USD.TSY.ZERO.2Y | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.3M | IR.USD.TSY.ZERO.3M | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.3Y | IR.USD.TSY.ZERO.3Y | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.5Y | IR.USD.TSY.ZERO.5Y | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.6M | IR.USD.TSY.ZERO.6M | 1 | REFERENCE_NAME_MAPPING |
| FO | USD.TSY.ZERO.7Y | IR.USD.TSY.ZERO.7Y | 1 | REFERENCE_NAME_MAPPING |
| FRTB | GIRR\|USD\|USD_TSY\|0.25Y | IR.USD.TSY.ZERO.3M | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|0.5Y | IR.USD.TSY.ZERO.6M | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|10Y | IR.USD.TSY.ZERO.10Y | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|1Y | IR.USD.TSY.ZERO.1Y | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|2Y | IR.USD.TSY.ZERO.2Y | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|3Y | IR.USD.TSY.ZERO.3Y | 1 | EXACT_TENOR |
| FRTB | GIRR\|USD\|USD_TSY\|5Y | IR.USD.TSY.ZERO.5Y | 1 | EXACT_TENOR |

**mapping_exceptions** — 0 rows (`04_mapping_exceptions.csv`)

_(empty)_


## Step 05: Scenario shocks in canonical units

Scenario sets {'STRESS_HYPO': 12, 'SVAR_1D': 250, 'VAR_1D': 260}. Rate shocks are absolute, in bp, on zero rates. Shown: per-factor statistics and a sample of the scenario matrix.

**scenario_statistics** — 33 rows (`05_scenario_statistics.csv`)

| scenario_set | rf_id | n | mean_bp | std_bp | min_bp | p01_bp | p99_bp | max_bp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| STRESS_HYPO | IR.USD.TSY.ZERO.1M | 12 | 46.25 | 108.46 | -100.00 | -92.3 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.3M | 12 | 46.6388 | 108.77 | -100.00 | -92.3 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.6M | 12 | 46.8842 | 109.09 | -100.00 | -92.3 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.1Y | 12 | 47.1295 | 109.49 | -100.00 | -92.4219 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.2Y | 12 | 48.4165 | 109.56 | -100.00 | -93.0696 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.3Y | 12 | 48.56 | 109.63 | -100.00 | -93.4485 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.5Y | 12 | 48.7408 | 110.35 | -100.00 | -93.9258 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.7Y | 12 | 46.221 | 111.66 | -100.00 | -94.2402 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.10Y | 12 | 42.3889 | 114.36 | -100.00 | -94.5734 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.20Y | 12 | 39.8565 | 115.79 | -100.00 | -95.2211 | 289.00 | 300.00 |
| STRESS_HYPO | IR.USD.TSY.ZERO.30Y | 12 | 40 | 116.29 | -100.00 | -95.6 | 289.00 | 300.00 |
| SVAR_1D | IR.USD.TSY.ZERO.1M | 250 | -0.3206 | 7.59 | -34.67 | -18.1522 | 17.63 | 49.81 |
| SVAR_1D | IR.USD.TSY.ZERO.3M | 250 | -0.1156 | 7.38 | -27.95 | -18.6864 | 19.3 | 46.24 |
| SVAR_1D | IR.USD.TSY.ZERO.6M | 250 | -0.0967 | 7.94 | -27.19 | -20.5908 | 22.42 | 38.73 |
| SVAR_1D | IR.USD.TSY.ZERO.1Y | 250 | -0.106 | 8.4 | -26.25 | -23.3819 | 22.77 | 31.95 |
| SVAR_1D | IR.USD.TSY.ZERO.2Y | 250 | 0.1244 | 9.04 | -28.38 | -27.2051 | 23.28 | 28.54 |
| SVAR_1D | IR.USD.TSY.ZERO.3Y | 250 | 0.1205 | 8.82 | -27.88 | -24.7188 | 23.29 | 26.44 |
| SVAR_1D | IR.USD.TSY.ZERO.5Y | 250 | 0.0104 | 8.7 | -30.71 | -26.3391 | 21.39 | 26.16 |
| SVAR_1D | IR.USD.TSY.ZERO.7Y | 250 | 0.1396 | 8.42 | -27.8 | -24.8247 | 20.55 | 25.4 |
| SVAR_1D | IR.USD.TSY.ZERO.10Y | 250 | 0.1258 | 8.15 | -28.26 | -23.8304 | 22.26 | 27.56 |
| SVAR_1D | IR.USD.TSY.ZERO.20Y | 250 | 0.0819 | 7.94 | -27.53 | -22.0203 | 21.32 | 25.64 |
| SVAR_1D | IR.USD.TSY.ZERO.30Y | 250 | 0.174 | 8.12 | -28.66 | -20.4886 | 20.78 | 22.83 |
| VAR_1D | IR.USD.TSY.ZERO.1M | 260 | -0.1474 | 3.81 | -13.46 | -11.0853 | 7.67 | 19.24 |
| VAR_1D | IR.USD.TSY.ZERO.3M | 260 | -0.1223 | 3.83 | -15.6 | -10.3743 | 8.79 | 15.92 |
| VAR_1D | IR.USD.TSY.ZERO.6M | 260 | -0.0673 | 4.41 | -17.9 | -10.7267 | 10.44 | 12.21 |
| VAR_1D | IR.USD.TSY.ZERO.1Y | 260 | -0.0463 | 4.91 | -21.36 | -12.865 | 11.7 | 14.49 |
| VAR_1D | IR.USD.TSY.ZERO.2Y | 260 | 0.0589 | 5.22 | -22.66 | -13.6633 | 11.7 | 14.03 |
| VAR_1D | IR.USD.TSY.ZERO.3Y | 260 | -0.0589 | 5.14 | -22.9 | -13.8534 | 11.95 | 13.47 |
| VAR_1D | IR.USD.TSY.ZERO.5Y | 260 | 0.1292 | 5.08 | -22.02 | -13.9072 | 10.98 | 13.65 |
| VAR_1D | IR.USD.TSY.ZERO.7Y | 260 | 0.1321 | 5.01 | -21.05 | -14.3796 | 10.78 | 12.82 |
| VAR_1D | IR.USD.TSY.ZERO.10Y | 260 | 0.1361 | 4.94 | -18.59 | -13.9211 | 11.16 | 12.92 |
| VAR_1D | IR.USD.TSY.ZERO.20Y | 260 | 0.152 | 4.84 | -17.77 | -13.7657 | 11.76 | 13.46 |
| VAR_1D | IR.USD.TSY.ZERO.30Y | 260 | 0.2509 | 4.93 | -18.07 | -13.8195 | 11.28 | 14.55 |

**scenario_matrix_sample** — 20 rows (`05_scenario_matrix_sample.csv`)

| scenario_set | scenario_id | scenario_name | 1M | 3M | 6M | 1Y | 2Y | 3Y | 5Y | 7Y | 10Y | 20Y | 30Y |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| VAR_1D | VAR_253 | HIST_1D_2026-11-13 | -4.18 | -3.88 | -2.53 | -2.78 | -2.52 | -1.84 | -0.66 | 0.89 | 0.68 | 3.69 | 0.62 |
| VAR_1D | VAR_254 | HIST_1D_2026-11-16 | -4.34 | -1.38 | -0.67 | -0.26 | -0.87 | -0.74 | 0.47 | 1.65 | 3.26 | 5.62 | 2.38 |
| VAR_1D | VAR_255 | HIST_1D_2026-11-17 | -11.03 | -10.29 | -10.58 | -10.45 | -9.39 | -7.82 | -9.16 | -8.9 | -8.55 | -7.49 | -7.35 |
| VAR_1D | VAR_256 | HIST_1D_2026-11-18 | 0.39 | -0.45 | -1.39 | -4.07 | -2.51 | -4.01 | -2.9 | -1.5 | -2.22 | -1.33 | -1.7 |
| VAR_1D | VAR_257 | HIST_1D_2026-11-19 | -0.15 | 0.74 | 2.09 | 2.04 | 4.32 | 3.35 | 3.49 | 4.07 | 1.62 | 3.45 | 2.09 |
| VAR_1D | VAR_258 | HIST_1D_2026-11-20 | -4.64 | -4.28 | -5.21 | -7.36 | -5.19 | -6.38 | -3.36 | -3.51 | -3.84 | -1.77 | -1.05 |
| VAR_1D | VAR_259 | HIST_1D_2026-11-23 | 4.51 | 5.09 | 8.66 | 9.58 | 10.03 | 9.61 | 8.47 | 10.88 | 11.13 | 11.76 | 11.33 |
| VAR_1D | VAR_260 | HIST_1D_2026-11-24 | -1.51 | -0.37 | -0.57 | 1.08 | 1.49 | 1.93 | 3.59 | 4.26 | 5.13 | 4.4 | 6.19 |
| STRESS_HYPO | HYP_001 | PARALLEL_UP_10BP | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10 |
| STRESS_HYPO | HYP_002 | PARALLEL_UP_25BP | 25 | 25 | 25 | 25 | 25 | 25 | 25 | 25 | 25 | 25 | 25 |
| STRESS_HYPO | HYP_003 | PARALLEL_DOWN_15BP | -15 | -15 | -15 | -15 | -15 | -15 | -15 | -15 | -15 | -15 | -15 |
| STRESS_HYPO | HYP_004 | PARALLEL_DOWN_30BP | -30 | -30 | -30 | -30 | -30 | -30 | -30 | -30 | -30 | -30 | -30 |
| STRESS_HYPO | HYP_005 | PARALLEL_UP_100BP | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 | 100.00 |
| STRESS_HYPO | HYP_006 | PARALLEL_DOWN_100BP | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 | -100.00 |
| STRESS_HYPO | HYP_007 | PARALLEL_UP_200BP | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 | 200.00 |
| STRESS_HYPO | HYP_008 | PARALLEL_UP_300BP_BEYOND_GRID | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 |
| STRESS_HYPO | HYP_009 | BEAR_STEEPENER_25_100 | 25 | 39 | 47.83 | 56.66 | 65.49 | 70.66 | 77.17 | 81.46 | 86 | 94.83 | 100.00 |
| STRESS_HYPO | HYP_010 | BULL_FLATTENER_10_60 | -10 | -19.33 | -25.22 | -31.11 | -37 | -40.44 | -44.78 | -47.64 | -50.67 | -56.56 | -60 |
| STRESS_HYPO | HYP_011 | TWIST_2S10S_50 | 50 | 50 | 50 | 50 | 50 | 37.5 | 12.5 | -12.5 | -50 | -50 | -50 |
| STRESS_HYPO | HYP_012 | BELLY_UP_50 | 0 | 0 | 0 | 0 | 12.5 | 25 | 50 | 43.33 | 33.33 | 0 | 0 |


## Step 06: Sensitivities: unit conversion and reconciliation

FRTB GIRR delta (per unit of rate, MAR21.19) and FO DV01 (positive = loss) are converted to one convention: dPV per +1bp. Each feed is then reconciled with the engine's own bump-and-revalue numbers. A BREAK means the feed disagrees with the engine by more than tolerance (one FO value is deliberately stale in the sample data).

**sensitivities_canonical** — 52 rows (`06_sensitivities_canonical.csv`)

| position_id | trade_id | sens_source | sens_rf_key | measure | value_canonical | sensitivity_type | sensitivity_value | unit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.1Y | DELTA | -56.07 | DV01 | 56.07 | USD_PER_BP_LOSS_POSITIVE |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.2Y | DELTA | -111.66 | DV01 | 111.66 | USD_PER_BP_LOSS_POSITIVE |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.3Y | DELTA | -299.24 | DV01 | 299.24 | USD_PER_BP_LOSS_POSITIVE |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.5Y | DELTA | -4,475.68 | DV01 | 4,475.68 | USD_PER_BP_LOSS_POSITIVE |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.6M | DELTA | -2.78 | DV01 | 2.78 | USD_PER_BP_LOSS_POSITIVE |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.1Y | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.2Y | GAMMA | 0.02 | GAMMA | 0.02 | USD_PER_BP2 |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.3Y | GAMMA | 0.07 | GAMMA | 0.07 | USD_PER_BP2 |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.5Y | GAMMA | 2.18 | GAMMA | 2.18 | USD_PER_BP2 |
| POS-0001 | TRD-BOND-0001 | FO | USD.TSY.ZERO.6M | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0001 | TRD-BOND-0001 | FRTB | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | -2.78 | GIRR_DELTA | -27,829.31 | USD_PER_UNIT_RATE |
| POS-0001 | TRD-BOND-0001 | FRTB | GIRR\|USD\|USD_TSY\|1Y | DELTA | -56.07 | GIRR_DELTA | -560,696.86 | USD_PER_UNIT_RATE |
| POS-0001 | TRD-BOND-0001 | FRTB | GIRR\|USD\|USD_TSY\|2Y | DELTA | -111.65 | GIRR_DELTA | -1,116,538.00 | USD_PER_UNIT_RATE |
| POS-0001 | TRD-BOND-0001 | FRTB | GIRR\|USD\|USD_TSY\|3Y | DELTA | -299.20 | GIRR_DELTA | -2,992,023.09 | USD_PER_UNIT_RATE |
| POS-0001 | TRD-BOND-0001 | FRTB | GIRR\|USD\|USD_TSY\|5Y | DELTA | -4,474.59 | GIRR_DELTA | -44,745,902.35 | USD_PER_UNIT_RATE |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.1Y | DELTA | 143.38 | DV01 | -143.38 | USD_PER_BP_LOSS_POSITIVE |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.2Y | DELTA | 3,690.95 | DV01 | -3,690.95 | USD_PER_BP_LOSS_POSITIVE |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.3M | DELTA | 1.53 | DV01 | -1.53 | USD_PER_BP_LOSS_POSITIVE |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.6M | DELTA | 16.48 | DV01 | -16.48 | USD_PER_BP_LOSS_POSITIVE |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.1Y | GAMMA | -0.01 | GAMMA | -0.01 | USD_PER_BP2 |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.2Y | GAMMA | -0.71 | GAMMA | -0.71 | USD_PER_BP2 |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.3M | GAMMA | -0 | GAMMA | -0 | USD_PER_BP2 |
| POS-0002 | TRD-BOND-0002 | FO | USD.TSY.ZERO.6M | GAMMA | -0 | GAMMA | -0 | USD_PER_BP2 |
| POS-0002 | TRD-BOND-0002 | FRTB | GIRR\|USD\|USD_TSY\|0.25Y | DELTA | 1.53 | GIRR_DELTA | 15,258.97 | USD_PER_UNIT_RATE |
| POS-0002 | TRD-BOND-0002 | FRTB | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | 16.48 | GIRR_DELTA | 164,781.45 | USD_PER_UNIT_RATE |
| POS-0002 | TRD-BOND-0002 | FRTB | GIRR\|USD\|USD_TSY\|1Y | DELTA | 139.20 | GIRR_DELTA | 1,391,981.02 | USD_PER_UNIT_RATE |
| POS-0002 | TRD-BOND-0002 | FRTB | GIRR\|USD\|USD_TSY\|2Y | DELTA | 3,690.59 | GIRR_DELTA | 36,905,920.74 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.10Y | DELTA | -3,220.28 | DV01 | 3,220.28 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.1M | DELTA | -0.29 | DV01 | 0.29 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.1Y | DELTA | -17.74 | DV01 | 17.74 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.2Y | DELTA | -39.39 | DV01 | 39.39 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.3M | DELTA | -2.13 | DV01 | 2.13 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.3Y | DELTA | -93.79 | DV01 | 93.79 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.5Y | DELTA | -176.14 | DV01 | 176.14 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.6M | DELTA | -4.11 | DV01 | 4.11 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.7Y | DELTA | -590.58 | DV01 | 590.58 | USD_PER_BP_LOSS_POSITIVE |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.10Y | GAMMA | 2.8 | GAMMA | 2.8 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.1M | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.1Y | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.2Y | GAMMA | 0.01 | GAMMA | 0.01 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.3M | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.3Y | GAMMA | 0.02 | GAMMA | 0.02 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.5Y | GAMMA | 0.06 | GAMMA | 0.06 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.6M | GAMMA | 0 | GAMMA | 0 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FO | USD.TSY.ZERO.7Y | GAMMA | 0.17 | GAMMA | 0.17 | USD_PER_BP2 |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|0.25Y | DELTA | -2.42 | GIRR_DELTA | -24,231.98 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | -4.11 | GIRR_DELTA | -41,120.30 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|10Y | DELTA | -3,454.97 | GIRR_DELTA | -34,549,743.10 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|1Y | DELTA | -17.74 | GIRR_DELTA | -177,417.19 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|2Y | DELTA | -39.39 | GIRR_DELTA | -393,868.95 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|3Y | DELTA | -93.78 | GIRR_DELTA | -937,825.92 | USD_PER_UNIT_RATE |
| POS-0003 | TRD-BOND-0003 | FRTB | GIRR\|USD\|USD_TSY\|5Y | DELTA | -530.42 | GIRR_DELTA | -5,304,209.82 | USD_PER_UNIT_RATE |

**sensitivity_reconciliation** — 52 rows (`06_sensitivity_reconciliation.csv`)

| position_id | comparison | risk_factor | measure | source_value | engine_value | diff | rel_diff | tolerance | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | DELTA | -56.07 | -56.07 | 0.0022 | 0.000039 | 0.5607 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | DELTA | -111.66 | -111.66 | 0.0043 | 0.000038 | 1.1166 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3Y | DELTA | -299.24 | -299.24 | -0.004 | -0.000013 | 2.9924 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.5Y | DELTA | -4,475.68 | -4,475.68 | 0.0011 | 0 | 44.7568 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | DELTA | -2.78 | -2.78 | 0.0029 | 0.001056 | 0.5 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | GAMMA | 0 | 0 | -0 | -0.000012 | 0.005 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | GAMMA | 0.02 | 0.02 | -0 | -0.000015 | 0.005 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3Y | GAMMA | 0.07 | 0.07 | 0 | 0.000006 | 0.005 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.5Y | GAMMA | 2.18 | 2.18 | 0 | 0 | 0.0218 | PASS |
| POS-0001 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | GAMMA | 0 | 0 | -0 | -0.028761 | 0.005 | PASS |
| POS-0001 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | -2.78 | -2.78 | -0 | -0 | 0.5 | PASS |
| POS-0001 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|1Y | DELTA | -56.07 | -56.07 | -0 | -0 | 0.5607 | PASS |
| POS-0001 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|2Y | DELTA | -111.65 | -111.65 | 0 | 0 | 1.1165 | PASS |
| POS-0001 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|3Y | DELTA | -299.20 | -299.20 | -0 | -0 | 2.992 | PASS |
| POS-0001 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|5Y | DELTA | -4,474.59 | -4,474.59 | 0 | 0 | 44.7459 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | DELTA | 143.38 | 139.20 | 4.1793 | 0.030023 | 1.392 | BREAK |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | DELTA | 3,690.95 | 3,690.95 | 0.0023 | 0.000001 | 36.9095 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3M | DELTA | 1.53 | 1.53 | 0.0041 | 0.002687 | 0.5 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | DELTA | 16.48 | 16.48 | 0.0015 | 0.000093 | 0.5 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | GAMMA | -0.01 | -0.01 | -0 | -0.000011 | 0.005 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | GAMMA | -0.71 | -0.71 | -0 | -0.000001 | 0.0071 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3M | GAMMA | -0 | -0 | -0 | -0.033012 | 0.005 | PASS |
| POS-0002 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | GAMMA | -0 | -0 | -0 | -0 | 0.005 | PASS |
| POS-0002 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|0.25Y | DELTA | 1.53 | 1.53 | -0 | -0 | 0.5 | PASS |
| POS-0002 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | 16.48 | 16.48 | 0 | 0 | 0.5 | PASS |
| POS-0002 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|1Y | DELTA | 139.20 | 139.20 | 0 | 0 | 1.392 | PASS |
| POS-0002 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|2Y | DELTA | 3,690.59 | 3,690.59 | -0 | -0 | 36.9059 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.10Y | DELTA | -3,220.28 | -3,220.28 | 0.0017 | 0.000001 | 32.2028 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1M | DELTA | -0.29 | -0.29 | -0.0012 | -0.004173 | 0.5 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | DELTA | -17.74 | -17.74 | 0.0023 | 0.000132 | 0.5 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | DELTA | -39.39 | -39.39 | -0.0006 | -0.000014 | 0.5 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3M | DELTA | -2.13 | -2.13 | 0.0044 | 0.002076 | 0.5 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3Y | DELTA | -93.79 | -93.79 | 0.0025 | 0.000026 | 0.9379 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.5Y | DELTA | -176.14 | -176.14 | 0.0026 | 0.000015 | 1.7614 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | DELTA | -4.11 | -4.11 | 0.0021 | 0.000514 | 0.5 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.7Y | DELTA | -590.58 | -590.58 | 0.0041 | 0.000007 | 5.9058 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.10Y | GAMMA | 2.8 | 2.8 | 0 | 0 | 0.028 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1M | GAMMA | 0 | 0 | 0 | 0.263226 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.1Y | GAMMA | 0 | 0 | 0 | 0.000221 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.2Y | GAMMA | 0.01 | 0.01 | 0 | 0.000085 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3M | GAMMA | 0 | 0 | -0 | -0.00618 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.3Y | GAMMA | 0.02 | 0.02 | 0 | 0.000018 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.5Y | GAMMA | 0.06 | 0.06 | 0 | 0.000007 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.6M | GAMMA | 0 | 0 | 0 | 0.002514 | 0.005 | PASS |
| POS-0003 | FO_vs_ENGINE | IR.USD.TSY.ZERO.7Y | GAMMA | 0.17 | 0.17 | 0 | 0.000002 | 0.005 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|0.25Y | DELTA | -2.42 | -2.42 | -0 | -0 | 0.5 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|0.5Y | DELTA | -4.11 | -4.11 | 0 | 0 | 0.5 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|10Y | DELTA | -3,454.97 | -3,454.97 | 0 | 0 | 34.5497 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|1Y | DELTA | -17.74 | -17.74 | -0 | -0 | 0.5 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|2Y | DELTA | -39.39 | -39.39 | 0 | 0 | 0.5 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|3Y | DELTA | -93.78 | -93.78 | -0 | -0 | 0.9378 | PASS |
| POS-0003 | FRTB_vs_ENGINE | GIRR\|USD\|USD_TSY\|5Y | DELTA | -530.42 | -530.42 | -0 | -0 | 5.3042 | PASS |


## Step 07: Pre-computed revaluation tables (grid and ladders)

Computed once per day by full revaluation, then re-used for every scenario set. Grid: PnL at parallel shifts + the DV01 weights that map a scenario onto the grid axis. Ladders: PnL when one risk factor moves alone. Shown for POS-0001.

**grid_nodes** — 23 rows (`07_grid_nodes.csv`)

| position_id | axis | node_bp | pnl |
| --- | --- | --- | --- |
| POS-0001 | PARALLEL_SHIFT_BP | -250.00 | 1,313,056.35 |
| POS-0001 | PARALLEL_SHIFT_BP | -200.00 | 1,037,769.20 |
| POS-0001 | PARALLEL_SHIFT_BP | -150.00 | 768,974.42 |
| POS-0001 | PARALLEL_SHIFT_BP | -100.00 | 506,515.68 |
| POS-0001 | PARALLEL_SHIFT_BP | -75 | 377,614.53 |
| POS-0001 | PARALLEL_SHIFT_BP | -50 | 250,240.48 |
| POS-0001 | PARALLEL_SHIFT_BP | -40 | 199,714.31 |
| POS-0001 | PARALLEL_SHIFT_BP | -30 | 149,428.33 |
| POS-0001 | PARALLEL_SHIFT_BP | -20 | 99,381.40 |
| POS-0001 | PARALLEL_SHIFT_BP | -10 | 49,572.34 |
| POS-0001 | PARALLEL_SHIFT_BP | -5 | 24,756.65 |
| POS-0001 | PARALLEL_SHIFT_BP | 0 | 0 |
| POS-0001 | PARALLEL_SHIFT_BP | 5 | -24,697.76 |
| POS-0001 | PARALLEL_SHIFT_BP | 10 | -49,336.77 |
| POS-0001 | PARALLEL_SHIFT_BP | 20 | -98,439.12 |
| POS-0001 | PARALLEL_SHIFT_BP | 30 | -147,308.17 |
| POS-0001 | PARALLEL_SHIFT_BP | 40 | -195,945.08 |
| POS-0001 | PARALLEL_SHIFT_BP | 50 | -244,350.96 |
| POS-0001 | PARALLEL_SHIFT_BP | 75 | -364,362.28 |
| POS-0001 | PARALLEL_SHIFT_BP | 100.00 | -482,954.08 |
| POS-0001 | PARALLEL_SHIFT_BP | 150.00 | -715,947.58 |
| POS-0001 | PARALLEL_SHIFT_BP | 200.00 | -943,466.34 |
| POS-0001 | PARALLEL_SHIFT_BP | 250.00 | -1,165,641.97 |

**grid_coordinate_weights** — 5 rows (`07_grid_coordinate_weights.csv`)

| rf_id | position_id | dv01_weight |
| --- | --- | --- |
| IR.USD.TSY.ZERO.6M | POS-0001 | 0.000563 |
| IR.USD.TSY.ZERO.1Y | POS-0001 | 0.011338 |
| IR.USD.TSY.ZERO.2Y | POS-0001 | 0.022579 |
| IR.USD.TSY.ZERO.3Y | POS-0001 | 0.060508 |
| IR.USD.TSY.ZERO.5Y | POS-0001 | 0.905012 |

**ladders_by_rf** — 23 rows (`07_ladders_by_rf.csv`)

| node_bp | 6M | 1Y | 2Y | 3Y | 5Y |
| --- | --- | --- | --- | --- | --- |
| -250.00 | 696.15 | 14,175.04 | 28,581.28 | 76,963.50 | 1,189,983.13 |
| -200.00 | 556.86 | 11,314.75 | 22,757.21 | 61,219.99 | 940,238.43 |
| -150.00 | 417.59 | 8,467.17 | 16,987.57 | 45,654.20 | 696,512.15 |
| -100.00 | 278.36 | 5,632.22 | 11,271.83 | 30,263.79 | 458,658.14 |
| -75 | 208.76 | 4,219.47 | 8,434.01 | 22,633.64 | 341,888.54 |
| -50 | 139.16 | 2,809.85 | 5,609.48 | 15,046.47 | 226,533.80 |
| -40 | 111.33 | 2,246.88 | 4,483.37 | 12,023.58 | 180,784.19 |
| -30 | 83.49 | 1,684.41 | 3,359.37 | 9,007.51 | 135,257.09 |
| -20 | 55.66 | 1,122.44 | 2,237.48 | 5,998.23 | 89,951.42 |
| -10 | 27.83 | 560.97 | 1,117.69 | 2,995.74 | 44,866.08 |
| -5 | 13.91 | 280.42 | 558.58 | 1,497.02 | 22,405.70 |
| 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | -13.91 | -280.30 | -558.06 | -1,495.34 | -22,351.15 |
| 10 | -27.83 | -560.47 | -1,115.60 | -2,988.99 | -44,647.90 |
| 20 | -55.66 | -1,120.45 | -2,229.10 | -5,971.26 | -89,078.68 |
| 30 | -83.48 | -1,679.92 | -3,340.52 | -8,946.81 | -133,293.42 |
| 40 | -111.31 | -2,238.90 | -4,449.86 | -11,915.68 | -177,293.17 |
| 50 | -139.13 | -2,797.39 | -5,557.12 | -14,877.87 | -221,078.99 |
| 75 | -208.68 | -4,191.43 | -8,316.20 | -22,254.26 | -329,614.44 |
| 100.00 | -278.23 | -5,582.37 | -11,062.37 | -29,589.32 | -436,835.59 |
| 150.00 | -417.29 | -8,355.00 | -16,516.27 | -44,136.54 | -647,399.01 |
| 200.00 | -556.32 | -11,115.35 | -21,919.29 | -58,521.65 | -852,895.30 |
| 250.00 | -695.32 | -13,863.46 | -27,271.94 | -72,746.78 | -1,053,447.45 |


## Step 08: PnL vectors (one value per position x scenario x method)

Long format: position_id, scenario_set, scenario_id, method, pnl. Validation checks that every valid position has a complete vector for every method and scenario set.

**vector_validation** — 36 rows (`08_vector_validation.csv`)

| position_id | method | scenario_set | n | n_distinct | n_bad | expected | status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POS-0001 | FULL_REVALUATION | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0001 | GRID | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0001 | LADDER | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0001 | TAYLOR | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0001 | FULL_REVALUATION | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0001 | GRID | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0001 | LADDER | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0001 | TAYLOR | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0001 | FULL_REVALUATION | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0001 | GRID | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0001 | LADDER | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0001 | TAYLOR | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0002 | FULL_REVALUATION | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0002 | GRID | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0002 | LADDER | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0002 | TAYLOR | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0002 | FULL_REVALUATION | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0002 | GRID | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0002 | LADDER | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0002 | TAYLOR | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0002 | FULL_REVALUATION | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0002 | GRID | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0002 | LADDER | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0002 | TAYLOR | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0003 | FULL_REVALUATION | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0003 | GRID | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0003 | LADDER | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0003 | TAYLOR | STRESS_HYPO | 12 | 12 | 0 | 12 | PASS |
| POS-0003 | FULL_REVALUATION | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0003 | GRID | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0003 | LADDER | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0003 | TAYLOR | SVAR_1D | 250 | 250 | 0 | 250 | PASS |
| POS-0003 | FULL_REVALUATION | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0003 | GRID | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0003 | LADDER | VAR_1D | 260 | 260 | 0 | 260 | PASS |
| POS-0003 | TAYLOR | VAR_1D | 260 | 260 | 0 | 260 | PASS |

**pnl_vectors_POS-0001** — 18 rows (`08_pnl_vectors_POS-0001.csv`)

| scenario_set | scenario_id | scenario_date | FULL_REVALUATION | TAYLOR | GRID | LADDER |
| --- | --- | --- | --- | --- | --- | --- |
| STRESS_HYPO | HYP_001 |  | -49,336.77 | -49,329.23 | -49,336.77 | -49,340.79 |
| STRESS_HYPO | HYP_002 |  | -122,902.73 | -122,896.47 | -122,873.65 | -122,899.65 |
| STRESS_HYPO | HYP_003 |  | 74,447.21 | 74,420.44 | 74,476.87 | 74,466.77 |
| STRESS_HYPO | HYP_004 |  | 149,428.33 | 149,352.81 | 149,428.33 | 149,391.88 |
| STRESS_HYPO | HYP_005 |  | -482,954.08 | -483,053.89 | -482,954.08 | -483,347.88 |
| STRESS_HYPO | HYP_006 |  | 506,515.68 | 505,805.90 | 506,515.68 | 506,104.34 |
| SVAR_1D | SVAR_001 | 2022-03-01 | -30,764.94 | -30,758.87 | -30,761.54 | -30,760.52 |
| SVAR_1D | SVAR_002 | 2022-03-02 | 54,469.99 | 54,453.12 | 54,478.02 | 54,477.80 |
| SVAR_1D | SVAR_003 | 2022-03-03 | -8,847.64 | -8,845.74 | -8,840.96 | -8,841.27 |
| SVAR_1D | SVAR_004 | 2022-03-04 | 12,373.73 | 12,370.61 | 12,381.23 | 12,380.49 |
| SVAR_1D | SVAR_005 | 2022-03-07 | 28,691.83 | 28,684.45 | 28,693.49 | 28,696.36 |
| SVAR_1D | SVAR_006 | 2022-03-08 | -105,189.44 | -105,183.02 | -105,174.20 | -105,196.24 |
| VAR_1D | VAR_001 | 2025-11-10 | -54,391.54 | -54,384.55 | -54,379.10 | -54,388.01 |
| VAR_1D | VAR_002 | 2025-11-12 | 3,221.21 | 3,220.49 | 3,224.57 | 3,224.21 |
| VAR_1D | VAR_003 | 2025-11-13 | 23,328.19 | 23,321.86 | 23,329.33 | 23,328.59 |
| VAR_1D | VAR_004 | 2025-11-14 | 273.23 | 273.16 | 273.53 | 273.79 |
| VAR_1D | VAR_005 | 2025-11-17 | 26,499.07 | 26,491.64 | 26,501.09 | 26,499.46 |
| VAR_1D | VAR_006 | 2025-11-18 | 29,510.02 | 29,501.85 | 29,513.92 | 29,513.35 |


## Step 09: Aggregation and risk measures (VaR, SVaR, ES, stress)

Vectors are summed scenario-by-scenario up the hierarchy ['POSITION', 'BOOK', 'DESK', 'PORTFOLIO']; VaR is taken on the summed vector (KTH_WORST, 99%, 1-day; 10-day shown with sqrt(10) scaling). Diversification = sum of position VaRs - portfolio VaR.

**risk_measures** — 56 rows (`09_risk_measures.csv`)

| level | node | method | scenario_set | n_scenarios | var_1d | var_10d_sqrt_scaled | es_1d | worst_loss | mean_pnl | std_pnl | var_scenario_id | var_scenario_date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POSITION | POS-0001 | FULL_REVALUATION | SVAR_1D | 250 | 105,688.21 | 334,215.46 | 103,831.92 | 128,811.07 | -1.36 | 42,952.13 | SVAR_180 | 2022-11-15 |
| POSITION | POS-0001 | GRID | SVAR_1D | 250 | 105,674.28 | 334,171.41 | 103,811.74 | 128,782.12 | 5.51 | 42,951.59 | SVAR_180 | 2022-11-15 |
| POSITION | POS-0001 | LADDER | SVAR_1D | 250 | 105,692.40 | 334,228.72 | 103,832.03 | 128,812.26 | 2.9 | 42,951.10 | SVAR_180 | 2022-11-15 |
| POSITION | POS-0001 | TAYLOR | SVAR_1D | 250 | 105,680.45 | 334,190.93 | 103,825.18 | 128,805.75 | -4.35 | 42,940.82 | SVAR_180 | 2022-11-15 |
| POSITION | POS-0002 | FULL_REVALUATION | SVAR_1D | 250 | 104,873.23 | 331,638.26 | 95,302.98 | 108,372.43 | 412.14 | 34,604.42 | SVAR_181 | 2022-11-16 |
| POSITION | POS-0002 | GRID | SVAR_1D | 250 | 104,879.49 | 331,658.06 | 95,308.82 | 108,376.74 | 409.64 | 34,604.41 | SVAR_181 | 2022-11-16 |
| POSITION | POS-0002 | LADDER | SVAR_1D | 250 | 104,869.01 | 331,624.92 | 95,298.71 | 108,366.54 | 410.94 | 34,604.28 | SVAR_181 | 2022-11-16 |
| POSITION | POS-0002 | TAYLOR | SVAR_1D | 250 | 104,851.79 | 331,570.47 | 95,283.58 | 108,350.98 | 413.39 | 34,600.96 | SVAR_181 | 2022-11-16 |
| POSITION | POS-0003 | FULL_REVALUATION | SVAR_1D | 250 | 98,342.14 | 310,985.14 | 86,006.38 | 111,161.10 | -377.54 | 33,791.62 | SVAR_071 | 2022-06-08 |
| POSITION | POS-0003 | GRID | SVAR_1D | 250 | 98,296.70 | 310,841.45 | 85,975.69 | 111,131.12 | -366.70 | 33,793.49 | SVAR_071 | 2022-06-08 |
| POSITION | POS-0003 | LADDER | SVAR_1D | 250 | 98,504.22 | 311,497.68 | 86,132.85 | 111,376.85 | -391.36 | 33,785.70 | SVAR_071 | 2022-06-08 |
| POSITION | POS-0003 | TAYLOR | SVAR_1D | 250 | 99,244.58 | 313,838.92 | 86,520.35 | 112,133.37 | -351.91 | 33,800.49 | SVAR_071 | 2022-06-08 |
| BOOK | RATES_BOOK_A | FULL_REVALUATION | SVAR_1D | 250 | 206,194.44 | 652,044.07 | 187,646.50 | 239,972.17 | -378.90 | 75,930.72 | SVAR_180 | 2022-11-15 |
| BOOK | RATES_BOOK_A | GRID | SVAR_1D | 250 | 206,157.62 | 651,927.63 | 187,595.42 | 239,913.24 | -361.19 | 75,932.24 | SVAR_180 | 2022-11-15 |
| BOOK | RATES_BOOK_A | LADDER | SVAR_1D | 250 | 206,347.08 | 652,526.77 | 187,771.04 | 240,189.11 | -388.46 | 75,923.40 | SVAR_180 | 2022-11-15 |
| BOOK | RATES_BOOK_A | TAYLOR | SVAR_1D | 250 | 207,669.15 | 656,707.51 | 188,153.21 | 240,939.12 | -356.26 | 75,953.97 | SVAR_180 | 2022-11-15 |
| BOOK | RATES_BOOK_B | FULL_REVALUATION | SVAR_1D | 250 | 104,873.23 | 331,638.26 | 95,302.98 | 108,372.43 | 412.14 | 34,604.42 | SVAR_181 | 2022-11-16 |
| BOOK | RATES_BOOK_B | GRID | SVAR_1D | 250 | 104,879.49 | 331,658.06 | 95,308.82 | 108,376.74 | 409.64 | 34,604.41 | SVAR_181 | 2022-11-16 |
| BOOK | RATES_BOOK_B | LADDER | SVAR_1D | 250 | 104,869.01 | 331,624.92 | 95,298.71 | 108,366.54 | 410.94 | 34,604.28 | SVAR_181 | 2022-11-16 |
| BOOK | RATES_BOOK_B | TAYLOR | SVAR_1D | 250 | 104,851.79 | 331,570.47 | 95,283.58 | 108,350.98 | 413.39 | 34,600.96 | SVAR_181 | 2022-11-16 |
| DESK | USD_RATES | FULL_REVALUATION | SVAR_1D | 250 | 123,860.75 | 391,682.08 | 112,140.05 | 135,358.65 | 33.25 | 44,211.09 | SVAR_180 | 2022-11-15 |
| DESK | USD_RATES | GRID | SVAR_1D | 250 | 123,828.31 | 391,579.49 | 112,111.91 | 135,306.88 | 48.45 | 44,212.88 | SVAR_180 | 2022-11-15 |
| DESK | USD_RATES | LADDER | SVAR_1D | 250 | 124,010.30 | 392,155.00 | 112,257.03 | 135,569.50 | 22.47 | 44,204.14 | SVAR_180 | 2022-11-15 |
| DESK | USD_RATES | TAYLOR | SVAR_1D | 250 | 125,335.76 | 396,346.48 | 112,565.95 | 136,322.69 | 57.13 | 44,272.24 | SVAR_180 | 2022-11-15 |
| PORTFOLIO | ALL | FULL_REVALUATION | SVAR_1D | 250 | 123,860.75 | 391,682.08 | 112,140.05 | 135,358.65 | 33.25 | 44,211.09 | SVAR_180 | 2022-11-15 |
| PORTFOLIO | ALL | GRID | SVAR_1D | 250 | 123,828.31 | 391,579.49 | 112,111.91 | 135,306.88 | 48.45 | 44,212.88 | SVAR_180 | 2022-11-15 |
| PORTFOLIO | ALL | LADDER | SVAR_1D | 250 | 124,010.30 | 392,155.00 | 112,257.03 | 135,569.50 | 22.47 | 44,204.14 | SVAR_180 | 2022-11-15 |
| PORTFOLIO | ALL | TAYLOR | SVAR_1D | 250 | 125,335.76 | 396,346.48 | 112,565.95 | 136,322.69 | 57.13 | 44,272.24 | SVAR_180 | 2022-11-15 |
| POSITION | POS-0001 | FULL_REVALUATION | VAR_1D | 260 | 54,742.18 | 173,109.96 | 56,706.69 | 66,886.73 | -534.25 | 25,073.23 | VAR_088 | 2026-03-19 |
| POSITION | POS-0001 | GRID | VAR_1D | 260 | 54,731.35 | 173,075.71 | 56,693.94 | 66,861.23 | -529.07 | 25,073.55 | VAR_088 | 2026-03-19 |
| POSITION | POS-0001 | LADDER | VAR_1D | 260 | 54,734.75 | 173,086.47 | 56,699.72 | 66,867.88 | -530.11 | 25,073.14 | VAR_088 | 2026-03-19 |
| POSITION | POS-0001 | TAYLOR | VAR_1D | 260 | 54,733.47 | 173,082.44 | 56,698.71 | 66,877.98 | -535.14 | 25,066.91 | VAR_088 | 2026-03-19 |
| POSITION | POS-0002 | FULL_REVALUATION | VAR_1D | 260 | 55,817.02 | 176,508.91 | 56,549.15 | 87,117.82 | 199.42 | 19,990.21 | VAR_162 | 2026-07-06 |
| POSITION | POS-0002 | GRID | VAR_1D | 260 | 55,826.05 | 176,537.46 | 56,554.74 | 87,124.65 | 197.72 | 19,990.31 | VAR_162 | 2026-07-06 |
| POSITION | POS-0002 | LADDER | VAR_1D | 260 | 55,822.61 | 176,526.61 | 56,550.96 | 87,116.17 | 198.20 | 19,990.15 | VAR_162 | 2026-07-06 |
| POSITION | POS-0002 | TAYLOR | VAR_1D | 260 | 55,808.42 | 176,481.72 | 56,539.96 | 87,100.77 | 199.84 | 19,988.17 | VAR_162 | 2026-07-06 |
| POSITION | POS-0003 | FULL_REVALUATION | VAR_1D | 260 | 46,603.45 | 147,373.06 | 45,662.79 | 52,883.52 | -488.51 | 20,415.76 | VAR_074 | 2026-02-27 |
| POSITION | POS-0003 | GRID | VAR_1D | 260 | 46,578.82 | 147,295.15 | 45,644.64 | 52,845.36 | -480.49 | 20,416.75 | VAR_074 | 2026-02-27 |
| POSITION | POS-0003 | LADDER | VAR_1D | 260 | 46,630.99 | 147,460.13 | 45,689.57 | 52,911.16 | -490.11 | 20,412.79 | VAR_074 | 2026-02-27 |
| POSITION | POS-0003 | TAYLOR | VAR_1D | 260 | 47,735.06 | 150,951.51 | 45,952.39 | 53,206.19 | -496.76 | 20,414.13 | VAR_074 | 2026-02-27 |
| BOOK | RATES_BOOK_A | FULL_REVALUATION | VAR_1D | 260 | 106,477.61 | 336,711.76 | 100,965.30 | 119,770.25 | -1,022.76 | 45,097.11 | VAR_088 | 2026-03-19 |
| BOOK | RATES_BOOK_A | GRID | VAR_1D | 260 | 106,437.25 | 336,584.14 | 100,934.29 | 119,706.59 | -1,009.56 | 45,098.40 | VAR_088 | 2026-03-19 |
| BOOK | RATES_BOOK_A | LADDER | VAR_1D | 260 | 106,492.05 | 336,757.44 | 100,985.00 | 119,779.05 | -1,020.22 | 45,094.02 | VAR_088 | 2026-03-19 |
| BOOK | RATES_BOOK_A | TAYLOR | VAR_1D | 260 | 106,641.44 | 337,229.85 | 101,247.10 | 120,084.18 | -1,031.90 | 45,101.27 | VAR_088 | 2026-03-19 |
| BOOK | RATES_BOOK_B | FULL_REVALUATION | VAR_1D | 260 | 55,817.02 | 176,508.91 | 56,549.15 | 87,117.82 | 199.42 | 19,990.21 | VAR_162 | 2026-07-06 |
| BOOK | RATES_BOOK_B | GRID | VAR_1D | 260 | 55,826.05 | 176,537.46 | 56,554.74 | 87,124.65 | 197.72 | 19,990.31 | VAR_162 | 2026-07-06 |
| BOOK | RATES_BOOK_B | LADDER | VAR_1D | 260 | 55,822.61 | 176,526.61 | 56,550.96 | 87,116.17 | 198.20 | 19,990.15 | VAR_162 | 2026-07-06 |
| BOOK | RATES_BOOK_B | TAYLOR | VAR_1D | 260 | 55,808.42 | 176,481.72 | 56,539.96 | 87,100.77 | 199.84 | 19,988.17 | VAR_162 | 2026-07-06 |
| DESK | USD_RATES | FULL_REVALUATION | VAR_1D | 260 | 59,590.04 | 188,440.25 | 56,200.09 | 72,680.79 | -823.34 | 26,565.24 | VAR_074 | 2026-02-27 |
| DESK | USD_RATES | GRID | VAR_1D | 260 | 59,547.93 | 188,307.08 | 56,173.84 | 72,623.59 | -811.83 | 26,566.41 | VAR_074 | 2026-02-27 |
| DESK | USD_RATES | LADDER | VAR_1D | 260 | 59,604.83 | 188,487.03 | 56,219.02 | 72,693.25 | -822.02 | 26,562.19 | VAR_074 | 2026-02-27 |
| DESK | USD_RATES | TAYLOR | VAR_1D | 260 | 60,715.32 | 191,998.69 | 56,616.39 | 72,996.54 | -832.06 | 26,601.50 | VAR_074 | 2026-02-27 |
| PORTFOLIO | ALL | FULL_REVALUATION | VAR_1D | 260 | 59,590.04 | 188,440.25 | 56,200.09 | 72,680.79 | -823.34 | 26,565.24 | VAR_074 | 2026-02-27 |
| PORTFOLIO | ALL | GRID | VAR_1D | 260 | 59,547.93 | 188,307.08 | 56,173.84 | 72,623.59 | -811.83 | 26,566.41 | VAR_074 | 2026-02-27 |
| PORTFOLIO | ALL | LADDER | VAR_1D | 260 | 59,604.83 | 188,487.03 | 56,219.02 | 72,693.25 | -822.02 | 26,562.19 | VAR_074 | 2026-02-27 |
| PORTFOLIO | ALL | TAYLOR | VAR_1D | 260 | 60,715.32 | 191,998.69 | 56,616.39 | 72,996.54 | -832.06 | 26,601.50 | VAR_074 | 2026-02-27 |

**diversification** — 8 rows (`09_diversification.csv`)

| method | scenario_set | sum_of_position_var_1d | portfolio_var_1d | diversification_benefit | diversification_pct |
| --- | --- | --- | --- | --- | --- |
| FULL_REVALUATION | SVAR_1D | 308,903.57 | 123,860.75 | 185,042.82 | 0.599031 |
| GRID | SVAR_1D | 308,850.46 | 123,828.31 | 185,022.16 | 0.599067 |
| LADDER | SVAR_1D | 309,065.63 | 124,010.30 | 185,055.33 | 0.598757 |
| TAYLOR | SVAR_1D | 309,776.82 | 125,335.76 | 184,441.06 | 0.5954 |
| FULL_REVALUATION | VAR_1D | 157,162.65 | 59,590.04 | 97,572.61 | 0.620838 |
| GRID | VAR_1D | 157,136.21 | 59,547.93 | 97,588.28 | 0.621043 |
| LADDER | VAR_1D | 157,188.35 | 59,604.83 | 97,583.52 | 0.620806 |
| TAYLOR | VAR_1D | 158,276.95 | 60,715.32 | 97,561.64 | 0.616398 |

**stress_results** — 48 rows (`09_stress_results.csv`)

| level | node | scenario_id | scenario_name | FULL_REVALUATION | TAYLOR | GRID | LADDER |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PORTFOLIO | ALL | HYP_001 | PARALLEL_UP_10BP | -52,150.14 | -52,162.97 | -52,150.14 | -52,187.61 |
| PORTFOLIO | ALL | HYP_002 | PARALLEL_UP_25BP | -129,379.72 | -129,543.08 | -129,314.09 | -129,555.79 |
| PORTFOLIO | ALL | HYP_003 | PARALLEL_DOWN_15BP | 79,232.11 | 79,108.81 | 79,300.00 | 79,205.15 |
| PORTFOLIO | ALL | HYP_004 | PARALLEL_DOWN_30BP | 159,686.19 | 159,254.83 | 159,686.19 | 159,342.91 |
| PORTFOLIO | ALL | HYP_005 | PARALLEL_UP_100BP | -498,101.55 | -500,885.37 | -498,101.55 | -501,701.61 |
| PORTFOLIO | ALL | HYP_006 | PARALLEL_DOWN_100BP | 551,759.36 | 546,983.92 | 551,759.36 | 547,822.89 |
| PORTFOLIO | ALL | HYP_007 | PARALLEL_UP_200BP | -946,889.09 | -955,672.19 | -946,889.09 | -960,672.48 |
| PORTFOLIO | ALL | HYP_008 | PARALLEL_UP_300BP_BEYOND_GRID | -1,350,349.52 | -1,364,360.47 | -1,361,209.12 | -1,389,575.44 |
| PORTFOLIO | ALL | HYP_009 | BEAR_STEEPENER_25_100 | -457,349.29 | -458,858.80 | -457,425.90 | -459,627.57 |
| PORTFOLIO | ALL | HYP_010 | BULL_FLATTENER_10_60 | 288,890.70 | 287,495.63 | 288,790.90 | 288,027.11 |
| PORTFOLIO | ALL | HYP_011 | TWIST_2S10S_50 | 279,345.24 | 279,024.03 | 278,417.54 | 279,179.29 |
| PORTFOLIO | ALL | HYP_012 | BELLY_UP_50 | -326,094.83 | -326,510.03 | -326,243.32 | -326,628.44 |
| POSITION | POS-0001 | HYP_001 | PARALLEL_UP_10BP | -49,336.77 | -49,329.23 | -49,336.77 | -49,340.79 |
| POSITION | POS-0001 | HYP_002 | PARALLEL_UP_25BP | -122,902.73 | -122,896.47 | -122,873.65 | -122,899.65 |
| POSITION | POS-0001 | HYP_003 | PARALLEL_DOWN_15BP | 74,447.21 | 74,420.44 | 74,476.87 | 74,466.77 |
| POSITION | POS-0001 | HYP_004 | PARALLEL_DOWN_30BP | 149,428.33 | 149,352.81 | 149,428.33 | 149,391.88 |
| POSITION | POS-0001 | HYP_005 | PARALLEL_UP_100BP | -482,954.08 | -483,053.89 | -482,954.08 | -483,347.88 |
| POSITION | POS-0001 | HYP_006 | PARALLEL_DOWN_100BP | 506,515.68 | 505,805.90 | 506,515.68 | 506,104.34 |
| POSITION | POS-0001 | HYP_007 | PARALLEL_UP_200BP | -943,466.34 | -943,355.77 | -943,466.34 | -945,007.91 |
| POSITION | POS-0001 | HYP_008 | PARALLEL_UP_300BP_BEYOND_GRID | -1,382,602.88 | -1,380,905.64 | -1,387,817.60 | -1,391,041.98 |
| POSITION | POS-0001 | HYP_009 | BEAR_STEEPENER_25_100 | -370,305.67 | -370,349.21 | -370,354.11 | -370,462.55 |
| POSITION | POS-0001 | HYP_010 | BULL_FLATTENER_10_60 | 220,815.46 | 220,675.75 | 220,811.38 | 220,771.01 |
| POSITION | POS-0001 | HYP_011 | TWIST_2S10S_50 | -75,421.93 | -75,427.48 | -75,388.97 | -75,422.69 |
| POSITION | POS-0001 | HYP_012 | BELLY_UP_50 | -229,885.06 | -229,855.26 | -230,049.67 | -229,931.99 |
| POSITION | POS-0002 | HYP_001 | PARALLEL_UP_10BP | 38,443.90 | 38,442.09 | 38,443.90 | 38,445.69 |
| POSITION | POS-0002 | HYP_002 | PARALLEL_UP_25BP | 95,968.90 | 95,970.75 | 95,959.53 | 95,971.15 |
| POSITION | POS-0002 | HYP_003 | PARALLEL_DOWN_15BP | -57,807.08 | -57,797.59 | -57,816.52 | -57,812.04 |
| POSITION | POS-0002 | HYP_004 | PARALLEL_DOWN_30BP | -115,784.09 | -115,756.54 | -115,784.09 | -115,767.92 |
| POSITION | POS-0002 | HYP_005 | PARALLEL_UP_100BP | 381,074.99 | 381,193.71 | 381,074.99 | 381,252.43 |
| POSITION | POS-0002 | HYP_006 | PARALLEL_DOWN_100BP | -388,604.95 | -388,365.14 | -388,604.95 | -388,424.08 |
| POSITION | POS-0002 | HYP_007 | PARALLEL_UP_200BP | 754,766.71 | 755,215.98 | 754,766.71 | 755,469.76 |
| POSITION | POS-0002 | HYP_008 | PARALLEL_UP_300BP_BEYOND_GRID | 1,121,219.00 | 1,122,066.83 | 1,123,011.10 | 1,124,494.66 |
| POSITION | POS-0002 | HYP_009 | BEAR_STEEPENER_25_100 | 248,877.55 | 248,913.47 | 248,827.71 | 248,891.29 |
| POSITION | POS-0002 | HYP_010 | BULL_FLATTENER_10_60 | -141,837.66 | -141,802.66 | -141,843.78 | -141,824.62 |
| POSITION | POS-0002 | HYP_011 | TWIST_2S10S_50 | 191,469.50 | 191,493.28 | 191,469.50 | 191,514.08 |
| POSITION | POS-0002 | HYP_012 | BELLY_UP_50 | 46,081.33 | 46,076.84 | 46,076.79 | 46,074.68 |
| POSITION | POS-0003 | HYP_001 | PARALLEL_UP_10BP | -41,257.27 | -41,275.83 | -41,257.27 | -41,292.51 |
| POSITION | POS-0003 | HYP_002 | PARALLEL_UP_25BP | -102,445.89 | -102,617.36 | -102,399.98 | -102,627.29 |
| POSITION | POS-0003 | HYP_003 | PARALLEL_DOWN_15BP | 62,591.99 | 62,485.95 | 62,639.65 | 62,550.42 |
| POSITION | POS-0003 | HYP_004 | PARALLEL_DOWN_30BP | 126,041.95 | 125,658.56 | 126,041.95 | 125,718.95 |
| POSITION | POS-0003 | HYP_005 | PARALLEL_UP_100BP | -396,222.46 | -399,025.19 | -396,222.46 | -399,606.16 |
| POSITION | POS-0003 | HYP_006 | PARALLEL_DOWN_100BP | 433,848.62 | 429,543.16 | 433,848.62 | 430,142.62 |
| POSITION | POS-0003 | HYP_007 | PARALLEL_UP_200BP | -758,189.46 | -767,532.41 | -758,189.46 | -771,134.33 |
| POSITION | POS-0003 | HYP_008 | PARALLEL_UP_300BP_BEYOND_GRID | -1,088,965.64 | -1,105,521.65 | -1,096,402.61 | -1,123,028.12 |
| POSITION | POS-0003 | HYP_009 | BEAR_STEEPENER_25_100 | -335,921.17 | -337,423.07 | -335,899.50 | -338,056.31 |
| POSITION | POS-0003 | HYP_010 | BULL_FLATTENER_10_60 | 209,912.90 | 208,622.54 | 209,823.30 | 209,080.73 |
| POSITION | POS-0003 | HYP_011 | TWIST_2S10S_50 | 163,297.68 | 162,958.23 | 162,337.00 | 163,087.90 |
| POSITION | POS-0003 | HYP_012 | BELLY_UP_50 | -142,291.10 | -142,731.60 | -142,270.44 | -142,771.12 |


## Step 10: Method comparison against full revaluation

Accuracy of Taylor, grid and ladder vs full revaluation on the VaR / SVaR sets: mean/max absolute differences, VaR difference, and FRTB PLA-test style statistics (Spearman correlation, KS distance; zones use MAR32 thresholds as an analogy only). Stress scenarios are compared one by one in step 09. Extrapolation flags show scenarios that fell outside the pre-computed grid / ladder range.

**method_comparison** — 42 rows (`10_method_comparison.csv`)

| level | node | scenario_set | method | n_scenarios | mean_abs_diff | max_abs_diff | rmse | var_method | var_full_reval | var_rel_diff | spearman | ks | plat_style_zone |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POSITION | POS-0001 | SVAR_1D | GRID | 250 | 7.1 | 32.84 | 10.15 | 105,674.28 | 105,688.21 | -0.000132 | 1 | 0.008 | GREEN |
| POSITION | POS-0001 | SVAR_1D | LADDER | 250 | 5.32 | 26.31 | 6.96 | 105,692.40 | 105,688.21 | 0.00004 | 0.999999 | 0.008 | GREEN |
| POSITION | POS-0001 | SVAR_1D | TAYLOR | 250 | 7.88 | 74.47 | 12.89 | 105,680.45 | 105,688.21 | -0.000073 | 1 | 0.008 | GREEN |
| POSITION | POS-0002 | SVAR_1D | GRID | 250 | 2.5 | 9.5 | 3.42 | 104,879.49 | 104,873.23 | 0.00006 | 1 | 0.008 | GREEN |
| POSITION | POS-0002 | SVAR_1D | LADDER | 250 | 1.77 | 11.94 | 2.37 | 104,869.01 | 104,873.23 | -0.00004 | 1 | 0.008 | GREEN |
| POSITION | POS-0002 | SVAR_1D | TAYLOR | 250 | 2.57 | 22.85 | 4.42 | 104,851.79 | 104,873.23 | -0.000204 | 1 | 0.008 | GREEN |
| POSITION | POS-0003 | SVAR_1D | GRID | 250 | 11.3 | 63.33 | 15.97 | 98,296.70 | 98,342.14 | -0.000462 | 0.999998 | 0.008 | GREEN |
| POSITION | POS-0003 | SVAR_1D | LADDER | 250 | 18.37 | 255.27 | 40.77 | 98,504.22 | 98,342.14 | 0.001648 | 1 | 0.008 | GREEN |
| POSITION | POS-0003 | SVAR_1D | TAYLOR | 250 | 763.79 | 3,197.84 | 1,023.26 | 99,244.58 | 98,342.14 | 0.009177 | 0.999043 | 0.02 | GREEN |
| BOOK | RATES_BOOK_A | SVAR_1D | GRID | 250 | 18.02 | 82.91 | 24.66 | 206,157.62 | 206,194.44 | -0.000179 | 1 | 0.008 | GREEN |
| BOOK | RATES_BOOK_A | SVAR_1D | LADDER | 250 | 18.49 | 261.65 | 41.22 | 206,347.08 | 206,194.44 | 0.00074 | 1 | 0.008 | GREEN |
| BOOK | RATES_BOOK_A | SVAR_1D | TAYLOR | 250 | 764.09 | 3,194.20 | 1,023.44 | 207,669.15 | 206,194.44 | 0.007152 | 0.999802 | 0.016 | GREEN |
| BOOK | RATES_BOOK_B | SVAR_1D | GRID | 250 | 2.5 | 9.5 | 3.42 | 104,879.49 | 104,873.23 | 0.00006 | 1 | 0.008 | GREEN |
| BOOK | RATES_BOOK_B | SVAR_1D | LADDER | 250 | 1.77 | 11.94 | 2.37 | 104,869.01 | 104,873.23 | -0.00004 | 1 | 0.008 | GREEN |
| BOOK | RATES_BOOK_B | SVAR_1D | TAYLOR | 250 | 2.57 | 22.85 | 4.42 | 104,851.79 | 104,873.23 | -0.000204 | 1 | 0.008 | GREEN |
| DESK | USD_RATES | SVAR_1D | GRID | 250 | 15.67 | 76.87 | 21.74 | 123,828.31 | 123,860.75 | -0.000262 | 1 | 0.008 | GREEN |
| DESK | USD_RATES | SVAR_1D | LADDER | 250 | 18.33 | 257.43 | 40.62 | 124,010.30 | 123,860.75 | 0.001207 | 1 | 0.008 | GREEN |
| DESK | USD_RATES | SVAR_1D | TAYLOR | 250 | 763.77 | 3,192.54 | 1,023.00 | 125,335.76 | 123,860.75 | 0.011909 | 0.999308 | 0.016 | GREEN |
| PORTFOLIO | ALL | SVAR_1D | GRID | 250 | 15.67 | 76.87 | 21.74 | 123,828.31 | 123,860.75 | -0.000262 | 1 | 0.008 | GREEN |
| PORTFOLIO | ALL | SVAR_1D | LADDER | 250 | 18.33 | 257.43 | 40.62 | 124,010.30 | 123,860.75 | 0.001207 | 1 | 0.008 | GREEN |
| PORTFOLIO | ALL | SVAR_1D | TAYLOR | 250 | 763.77 | 3,192.54 | 1,023.00 | 125,335.76 | 123,860.75 | 0.011909 | 0.999308 | 0.016 | GREEN |
| POSITION | POS-0001 | VAR_1D | GRID | 260 | 5.2 | 28.54 | 6.44 | 54,731.35 | 54,742.18 | -0.000198 | 1 | 0.007692 | GREEN |
| POSITION | POS-0001 | VAR_1D | LADDER | 260 | 4.31 | 20.06 | 5.19 | 54,734.75 | 54,742.18 | -0.000136 | 1 | 0.007692 | GREEN |
| POSITION | POS-0001 | VAR_1D | TAYLOR | 260 | 4.47 | 47.4 | 6.68 | 54,733.47 | 54,742.18 | -0.000159 | 1 | 0.007692 | GREEN |
| POSITION | POS-0002 | VAR_1D | GRID | 260 | 1.7 | 9.03 | 2.13 | 55,826.05 | 55,817.02 | 0.000162 | 1 | 0.007692 | GREEN |
| POSITION | POS-0002 | VAR_1D | LADDER | 260 | 1.37 | 5.74 | 1.63 | 55,822.61 | 55,817.02 | 0.0001 | 1 | 0.003846 | GREEN |
| POSITION | POS-0002 | VAR_1D | TAYLOR | 260 | 1.46 | 17.05 | 2.24 | 55,808.42 | 55,817.02 | -0.000154 | 1 | 0.003846 | GREEN |
| POSITION | POS-0003 | VAR_1D | GRID | 260 | 8.14 | 46.72 | 10.19 | 46,578.82 | 46,603.45 | -0.000529 | 0.999999 | 0.007692 | GREEN |
| POSITION | POS-0003 | VAR_1D | LADDER | 260 | 7.89 | 124.08 | 13.59 | 46,630.99 | 46,603.45 | 0.000591 | 0.999999 | 0.007692 | GREEN |
| POSITION | POS-0003 | VAR_1D | TAYLOR | 260 | 437.69 | 2,007.32 | 575.49 | 47,735.06 | 46,603.45 | 0.024282 | 0.999164 | 0.019231 | GREEN |
| BOOK | RATES_BOOK_A | VAR_1D | GRID | 260 | 13.25 | 75.27 | 16.14 | 106,437.25 | 106,477.61 | -0.000379 | 0.999999 | 0.007692 | GREEN |
| BOOK | RATES_BOOK_A | VAR_1D | LADDER | 260 | 9.54 | 125.72 | 14.5 | 106,492.05 | 106,477.61 | 0.000136 | 0.999999 | 0.007692 | GREEN |
| BOOK | RATES_BOOK_A | VAR_1D | TAYLOR | 260 | 437.94 | 1,993.97 | 575.53 | 106,641.44 | 106,477.61 | 0.001539 | 0.999816 | 0.019231 | GREEN |
| BOOK | RATES_BOOK_B | VAR_1D | GRID | 260 | 1.7 | 9.03 | 2.13 | 55,826.05 | 55,817.02 | 0.000162 | 1 | 0.007692 | GREEN |
| BOOK | RATES_BOOK_B | VAR_1D | LADDER | 260 | 1.37 | 5.74 | 1.63 | 55,822.61 | 55,817.02 | 0.0001 | 1 | 0.003846 | GREEN |
| BOOK | RATES_BOOK_B | VAR_1D | TAYLOR | 260 | 1.46 | 17.05 | 2.24 | 55,808.42 | 55,817.02 | -0.000154 | 1 | 0.003846 | GREEN |
| DESK | USD_RATES | VAR_1D | GRID | 260 | 11.59 | 66.24 | 14.31 | 59,547.93 | 59,590.04 | -0.000707 | 1 | 0.007692 | GREEN |
| DESK | USD_RATES | VAR_1D | LADDER | 260 | 8.94 | 124.07 | 14.04 | 59,604.83 | 59,590.04 | 0.000248 | 0.999999 | 0.007692 | GREEN |
| DESK | USD_RATES | VAR_1D | TAYLOR | 260 | 437.72 | 1,997.27 | 575.31 | 60,715.32 | 59,590.04 | 0.018884 | 0.999684 | 0.019231 | GREEN |
| PORTFOLIO | ALL | VAR_1D | GRID | 260 | 11.59 | 66.24 | 14.31 | 59,547.93 | 59,590.04 | -0.000707 | 1 | 0.007692 | GREEN |
| PORTFOLIO | ALL | VAR_1D | LADDER | 260 | 8.94 | 124.07 | 14.04 | 59,604.83 | 59,590.04 | 0.000248 | 0.999999 | 0.007692 | GREEN |
| PORTFOLIO | ALL | VAR_1D | TAYLOR | 260 | 437.72 | 1,997.27 | 575.31 | 60,715.32 | 59,590.04 | 0.018884 | 0.999684 | 0.019231 | GREEN |

**extrapolation_flags** — 6 rows (`10_extrapolation_flags.csv`)

| position_id | method | scenario_set | n_extrapolated |
| --- | --- | --- | --- |
| POS-0002 | GRID | STRESS_HYPO | 1 |
| POS-0002 | LADDER | STRESS_HYPO | 1 |
| POS-0001 | LADDER | STRESS_HYPO | 1 |
| POS-0003 | GRID | STRESS_HYPO | 1 |
| POS-0003 | LADDER | STRESS_HYPO | 1 |
| POS-0001 | GRID | STRESS_HYPO | 1 |


## Step 11: Scenario explain for POS-0001

How each method produced its number for selected scenarios (worst stressed day, worst VaR day, a curve twist and a beyond-grid stress).

**explain_shocks_bp** — 4 rows (`11_explain_shocks_bp.csv`)

| scenario | 6M | 1Y | 2Y | 3Y | 5Y |
| --- | --- | --- | --- | --- | --- |
| SVAR_WORST: SVAR_1D:SVAR_190 STRESSED_1D_2022-11-30 | 23.84 | 27.06 | 27.28 | 26.44 | 26.16 |
| VAR_WORST: VAR_1D:VAR_204 HIST_1D_2026-09-02 | 10.12 | 11.91 | 12.28 | 13.24 | 13.65 |
| STRESS_HYPO:HYP_011 TWIST_2S10S_50 | 50 | 50 | 50 | 37.5 | 12.5 |
| STRESS_HYPO:HYP_008 PARALLEL_UP_300BP_BEYOND_GRID | 300.00 | 300.00 | 300.00 | 300.00 | 300.00 |

**explain_methods** — 16 rows (`11_explain_methods.csv`)

| scenario | method | pnl | diff_vs_full_reval | how |
| --- | --- | --- | --- | --- |
| SVAR_WORST: SVAR_1D:SVAR_190 STRESSED_1D_2022-11-30 | FULL_REVALUATION | -128,811.07 | 0 | all cash flows repriced on the shocked curve (benchmark) |
| SVAR_WORST: SVAR_1D:SVAR_190 STRESSED_1D_2022-11-30 | TAYLOR | -128,805.75 | 5.32 | delta part -129,585.37 + gamma part 779.63 |
| SVAR_WORST: SVAR_1D:SVAR_190 STRESSED_1D_2022-11-30 | GRID | -128,782.12 | 28.95 | coordinate 26.209 bp (DV01-weighted average shock) |
| SVAR_WORST: SVAR_1D:SVAR_190 STRESSED_1D_2022-11-30 | LADDER | -128,812.26 | -1.19 | 6M -66.35; 1Y -1,515.51; 2Y -3,038.64; 3Y -7,886.09; 5Y -116,305.68 |
| VAR_WORST: VAR_1D:VAR_204 HIST_1D_2026-09-02 | FULL_REVALUATION | -66,886.73 | 0 | all cash flows repriced on the shocked curve (benchmark) |
| VAR_WORST: VAR_1D:VAR_204 HIST_1D_2026-09-02 | TAYLOR | -66,877.98 | 8.75 | delta part -67,088.97 + gamma part 210.99 |
| VAR_WORST: VAR_1D:VAR_204 HIST_1D_2026-09-02 | GRID | -66,861.23 | 25.5 | coordinate 13.569 bp (DV01-weighted average shock) |
| VAR_WORST: VAR_1D:VAR_204 HIST_1D_2026-09-02 | LADDER | -66,867.88 | 18.85 | 6M -28.16; 1Y -667.66; 2Y -1,368.92; 3Y -3,956.23; 5Y -60,846.92 |
| STRESS_HYPO:HYP_011 TWIST_2S10S_50 | FULL_REVALUATION | -75,421.93 | 0 | all cash flows repriced on the shocked curve (benchmark) |
| STRESS_HYPO:HYP_011 TWIST_2S10S_50 | TAYLOR | -75,427.48 | -5.55 | delta part -75,677.79 + gamma part 250.30 |
| STRESS_HYPO:HYP_011 TWIST_2S10S_50 | GRID | -75,388.97 | 32.97 | coordinate 15.306 bp (DV01-weighted average shock) |
| STRESS_HYPO:HYP_011 TWIST_2S10S_50 | LADDER | -75,422.69 | -0.75 | 6M -139.13; 1Y -2,797.39; 2Y -5,557.12; 3Y -11,173.46; 5Y -55,755.59 |
| STRESS_HYPO:HYP_008 PARALLEL_UP_300BP_BEYOND_GRID | FULL_REVALUATION | -1,382,602.88 | 0 | all cash flows repriced on the shocked curve (benchmark) |
| STRESS_HYPO:HYP_008 PARALLEL_UP_300BP_BEYOND_GRID | TAYLOR | -1,380,905.64 | 1,697.23 | delta part -1,483,289.69 + gamma part 102,384.04 |
| STRESS_HYPO:HYP_008 PARALLEL_UP_300BP_BEYOND_GRID | GRID | -1,387,817.60 | -5,214.73 | coordinate 300.000 bp (DV01-weighted average shock); EXTRAPOLATED beyond grid |
| STRESS_HYPO:HYP_008 PARALLEL_UP_300BP_BEYOND_GRID | LADDER | -1,391,041.98 | -8,439.10 | 6M -834.31; 1Y -16,611.56; 2Y -32,624.59; 3Y -86,971.91; 5Y -1,253,999.60; EXTRAPOLATED beyond ladder |

