# Sample data dictionary

All rows are synthetic teaching data.

- `positions.csv`: one trade-level fixed-rate bond position from the assumed CMCore source.
- `fo_sensitivities.csv`: separate front-office sensitivity snapshot from CMCore. It is retained for future reconciliation; it is not silently substituted for RDL.
- `frtb_sensitivities.csv`: RDL model sensitivity feed. `DV01` is stored as a positive loss-per-1bp convention; convexity is dimensionless under the formula documented in README.
- `scenario_shocks.csv`: MDSS scenario shocks. In v1 each scenario is one parallel USD OIS curve shock.
- `yield_curve_nodes.csv`: Reference curve nodes. Flat 5% zero rates are used in this simple illustration.
- `pricing_grid.csv`: Reference pricing grid for the same bond, generated with the same illustrative DCF function.

`risk_factor_key` is a governed identifier in a real bank. The demo mapping code constructs tenor keys from curve ID and tenor only to show the data flow.
