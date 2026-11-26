"""Step 4: risk-factor mapping.

Three mappings are produced:
* curve node -> scenario risk factor (built with the market state)
* position -> scenario risk factors it is exposed to (from the engine's own cash flows)
* sensitivity risk factor (FRTB vertex / FO bucket name) -> scenario risk factor(s) with
  interpolation weights, so that Taylor can apply scenario shocks to sensitivities that live on a
  different tenor grid (e.g. FRTB has a 15Y vertex, the scenario grid has 7Y but no 15Y).
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..curves import linear_interp_weights
from ..frtb import GIRR_VERTICES

SENS_MAP_SCHEMA = (
    "sens_source string, sens_rf_key string, rf_id string, weight double, mapping_rule string"
)


def frtb_sens_key(col_prefix: str = "") -> F.Column:
    c = lambda n: F.col(f"{col_prefix}{n}")  # noqa: E731
    return F.concat_ws("|", c("risk_class"), c("bucket"), c("risk_factor"), c("vertex"))


def map_sensitivity_risk_factors(
    spark: SparkSession, staged: dict[str, DataFrame]
) -> tuple[DataFrame, pd.DataFrame]:
    rf = staged["risk_factors"].toPandas()
    rows, exceptions = [], []

    # FRTB GIRR vertices -> scenario tenor risk factors on the same curve (linear weights)
    frtb_keys = (
        staged["frtb_sensitivities"]
        .select("risk_class", "bucket", "risk_factor", "vertex").distinct().toPandas()
    )
    for k in frtb_keys.itertuples(index=False):
        key = f"{k.risk_class}|{k.bucket}|{k.risk_factor}|{k.vertex}"
        crf = rf[(rf["curve_id"] == k.risk_factor) & (rf["rf_type"] == "ZERO_RATE")].sort_values("tenor_years")
        if k.risk_class != "GIRR" or crf.empty or k.vertex not in GIRR_VERTICES:
            exceptions.append({"sens_source": "FRTB", "sens_rf_key": key, "reason": "no scenario risk factors on this curve"})
            continue
        w = linear_interp_weights(crf["tenor_years"].to_numpy(float), [GIRR_VERTICES[k.vertex]])[:, 0]
        for rf_id, weight in zip(crf["rf_id"], w, strict=True):
            if weight > 0:
                rule = "EXACT_TENOR" if weight == 1.0 else "LINEAR_TENOR_INTERPOLATION"
                rows.append(("FRTB", key, rf_id, float(weight), rule))

    # FO bucket names -> engine risk factors via the governed Reference mapping table
    fo_names = staged["fo_sensitivities"].select("source_system", "risk_factor_name").distinct()
    name_map = staged["rf_name_mapping"].filter(F.col("mapping_status") == "ACTIVE")
    fo = (
        fo_names.join(name_map, (fo_names.source_system == name_map.source_system)
                      & (fo_names.risk_factor_name == name_map.source_risk_factor), "left")
        .select(fo_names.source_system, "risk_factor_name", "rf_id").toPandas()
    )
    for r in fo.itertuples(index=False):
        if pd.isna(r.rf_id):
            exceptions.append({"sens_source": r.source_system, "sens_rf_key": r.risk_factor_name,
                               "reason": "no ACTIVE rf_name_mapping row"})
        else:
            rows.append((r.source_system, r.risk_factor_name, r.rf_id, 1.0, "REFERENCE_NAME_MAPPING"))

    sens_map = spark.createDataFrame(rows, SENS_MAP_SCHEMA)
    exc = pd.DataFrame(exceptions, columns=["sens_source", "sens_rf_key", "reason"])
    return sens_map, exc
