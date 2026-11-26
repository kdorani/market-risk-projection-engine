"""Steps 3, 5, 7, 8: valuation, engine exposures, revaluation tables and PnL vectors.

Spark patterns used here:
* mapInPandas  - per-position work (no shuffle): pricing, bumps, grids, full revaluation
* groupBy().applyInPandas - per-position work that needs all rows of a position together
  (applying grids/ladders)
* join + groupBy aggregate - Taylor expansion is pure Spark SQL, no Python at all
Small reference data (market state, scenario matrix) travels inside the UDF closures.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..curves import MarketState, ScenarioShocks
from ..frtb import GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS
from ..methods import (
    RevaluationGrid,
    RevaluationLadder,
    apply_ladders,
    build_ladders,
    build_parallel_grid,
    bumped_rf_sensitivities,
    frtb_vertex_deltas,
    full_revaluation_pnl,
)
from ..products import PricerNotImplemented, build_instrument

LINEAGE_COLS = ["_source_system", "_source_object", "_load_ts", "run_id"]

VALUATION_SCHEMA = (
    "position_id string, product_type string, status string, message string, base_pv double, "
    "dirty_pv double, accrued_interest double, clean_pv double, clean_price_per_100 double, "
    "dirty_price_per_100 double, macaulay_duration_years double, n_future_cashflows int, "
    "final_maturity_years double"
)
CASHFLOW_SCHEMA = (
    "position_id string, period_start date, period_end date, pay_date date, accrual_factor double, "
    "coupon double, principal double, amount double, t_years double, zero_rate double, "
    "discount_factor double, pv double"
)
EXPOSURE_SCHEMA = "position_id string, rf_id string, delta_per_bp double, gamma_per_bp2 double"
VERTEX_SCHEMA = "position_id string, sens_rf_key string, engine_delta_per_bp double"
REVAL_SCHEMA = (
    "position_id string, table_type string, axis string, rf_id string, node_bp double, value double"
)
VECTOR_SCHEMA = (
    "position_id string, scenario_set string, scenario_id string, scenario_date string, "
    "method string, item string, value double"
)


def instrument_frame(staged: dict[str, DataFrame]) -> DataFrame:
    """Positions joined to their product terms (one row per position)."""
    pos = staged["positions"].drop(*LINEAGE_COLS)
    terms = staged["bond_terms"].drop("business_date", "currency", *LINEAGE_COLS)
    return pos.join(terms, "instrument_id", "left")


def _instruments(pdf: pd.DataFrame):
    for row in pdf.to_dict("records"):
        try:
            yield row, build_instrument(row), None
        except Exception as exc:  # reported by the valuation step
            yield row, None, exc


# --------------------------------------------------------------------------- valuation
def valuation_fn(market: MarketState):
    cols = [f.split()[0] for f in VALUATION_SCHEMA.split(", ")]

    def fn(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in batches:
            out = []
            for row, inst, err in _instruments(pdf):
                rec = {"position_id": row["position_id"], "product_type": row["product_type"]}
                try:
                    if err:
                        raise err
                    rec.update(inst.valuation_details(market))
                    rec.update(status="OK", message="", base_pv=inst.present_value(market))
                except PricerNotImplemented as exc:
                    rec.update(status="PRICER_NOT_IMPLEMENTED", message=str(exc))
                except Exception as exc:
                    rec.update(status="ERROR", message=f"{type(exc).__name__}: {exc}")
                out.append(rec)
            res = pd.DataFrame(out).reindex(columns=cols)
            res["n_future_cashflows"] = res["n_future_cashflows"].astype("Int64")
            yield res

    return fn


def cashflow_fn(market: MarketState):
    cols = [f.split()[0] for f in CASHFLOW_SCHEMA.split(", ")]

    def fn(batches):
        for pdf in batches:
            frames = []
            for row, inst, err in _instruments(pdf):
                if err is None and hasattr(inst, "cashflows"):
                    cf = inst.cashflows(market)
                    cf.insert(0, "position_id", row["position_id"])
                    frames.append(cf)
            yield pd.concat(frames)[cols] if frames else pd.DataFrame(columns=cols)

    return fn


# --------------------------------------------------------------------------- exposures
def exposure_fn(market: MarketState):
    def fn(batches):
        for pdf in batches:
            frames = []
            for row, inst, err in _instruments(pdf):
                if err is None:
                    s = bumped_rf_sensitivities(inst, market, inst.risk_factor_ids(market))
                    s.insert(0, "position_id", row["position_id"])
                    frames.append(s)
            yield pd.concat(frames) if frames else pd.DataFrame(
                columns=["position_id", "rf_id", "delta_per_bp", "gamma_per_bp2"])

    return fn


def vertex_fn(market: MarketState):
    def fn(batches):
        for pdf in batches:
            frames = []
            for row, inst, err in _instruments(pdf):
                if err is None and hasattr(inst, "discount_curve_id"):
                    cid = inst.discount_curve_id
                    v = frtb_vertex_deltas(inst, market, cid, GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS)
                    v = v[v["delta_per_bp"].abs() > 1e-6]
                    ccy = market.curve(cid).currency
                    frames.append(pd.DataFrame({
                        "position_id": row["position_id"],
                        "sens_rf_key": [f"GIRR|{ccy}|{cid}|{x}" for x in v["vertex"]],
                        "engine_delta_per_bp": v["delta_per_bp"].to_numpy(),
                    }))
            yield pd.concat(frames) if frames else pd.DataFrame(
                columns=["position_id", "sens_rf_key", "engine_delta_per_bp"])

    return fn


# --------------------------------------------------------------------------- grids & ladders
def revaluation_tables_fn(market: MarketState, grid_nodes: list[float], ladder_shifts: list[float]):
    def fn(batches):
        for pdf in batches:
            frames = []
            for row, inst, err in _instruments(pdf):
                if err is not None:
                    continue
                pid = row["position_id"]
                rf_ids = inst.risk_factor_ids(market)
                sens = bumped_rf_sensitivities(inst, market, rf_ids)
                grid = build_parallel_grid(inst, market, rf_ids, sens["delta_per_bp"].to_numpy(), grid_nodes)
                frames.append(pd.DataFrame({"position_id": pid, "table_type": "GRID_NODE", "axis": grid.axis,
                                            "rf_id": None, "node_bp": grid.nodes, "value": grid.pnl}))
                frames.append(pd.DataFrame({"position_id": pid, "table_type": "GRID_WEIGHT", "axis": grid.axis,
                                            "rf_id": grid.coord_rf_ids, "node_bp": np.nan,
                                            "value": grid.coord_weights}))
                for lad in build_ladders(inst, market, rf_ids, ladder_shifts):
                    frames.append(pd.DataFrame({"position_id": pid, "table_type": "LADDER", "axis": "RF_SHIFT_BP",
                                                "rf_id": lad.rf_id, "node_bp": lad.shifts, "value": lad.pnl}))
            yield pd.concat(frames) if frames else pd.DataFrame(
                columns=["position_id", "table_type", "axis", "rf_id", "node_bp", "value"])

    return fn


def apply_revaluation_tables_fn(shocks: ScenarioShocks, grid_policy: str, ladder_policy: str,
                                methods: list[str]):
    """Grouped by position: rebuild the grid/ladders from their table rows and apply them.
    Note: no pricer is needed here, only pre-computed PnL tables and the scenario shocks."""
    idx = shocks.index

    def long(pid, method, item, values):
        return pd.DataFrame({"position_id": pid, "scenario_set": idx["scenario_set"],
                             "scenario_id": idx["scenario_id"], "scenario_date": idx["scenario_date"],
                             "method": method, "item": item, "value": values})

    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        pid = pdf["position_id"].iloc[0]
        frames = []
        if "GRID" in methods:
            nodes = pdf[pdf.table_type == "GRID_NODE"].sort_values("node_bp")
            weights = pdf[pdf.table_type == "GRID_WEIGHT"]
            grid = RevaluationGrid(pid, nodes["axis"].iloc[0], nodes["node_bp"].to_numpy(),
                                   nodes["value"].to_numpy(), weights["rf_id"].tolist(),
                                   weights["value"].to_numpy())
            pnl, coord, flag = grid.apply(shocks, grid_policy)
            frames += [long(pid, "GRID", "PNL", pnl), long(pid, "GRID", "COORDINATE_BP", coord),
                       long(pid, "GRID", "EXTRAPOLATED", flag.astype(float))]
        if "LADDER" in methods:
            lads = [
                RevaluationLadder(pid, rf, g["node_bp"].to_numpy(), g["value"].to_numpy())
                for rf, g in pdf[pdf.table_type == "LADDER"].sort_values("node_bp").groupby("rf_id", sort=True)
            ]
            total, contrib, flag = apply_ladders(lads, shocks, ladder_policy)
            frames += [long(pid, "LADDER", "PNL", total), long(pid, "LADDER", "EXTRAPOLATED", flag.astype(float))]
            frames += [long(pid, "LADDER", f"CONTRIB|{lad.rf_id}", contrib[:, j]) for j, lad in enumerate(lads)]
        return pd.concat(frames) if frames else pd.DataFrame(
            columns=["position_id", "scenario_set", "scenario_id", "scenario_date", "method", "item", "value"])

    return fn


# --------------------------------------------------------------------------- full revaluation
def full_revaluation_fn(market: MarketState, shocks: ScenarioShocks):
    idx = shocks.index

    def fn(batches):
        for pdf in batches:
            frames = []
            for row, inst, err in _instruments(pdf):
                if err is None:
                    pnl = full_revaluation_pnl(inst, market, shocks)
                    frames.append(pd.DataFrame({
                        "position_id": row["position_id"], "scenario_set": idx["scenario_set"],
                        "scenario_id": idx["scenario_id"], "scenario_date": idx["scenario_date"],
                        "method": "FULL_REVALUATION", "item": "PNL", "value": pnl,
                    }))
            yield pd.concat(frames) if frames else pd.DataFrame(
                columns=["position_id", "scenario_set", "scenario_id", "scenario_date", "method", "item", "value"])

    return fn


# --------------------------------------------------------------------------- Taylor (Spark SQL)
def taylor_vectors(canon: DataFrame, sens_map: DataFrame, scen: DataFrame, valid_positions: DataFrame,
                   delta_source: str, gamma_source: str, include_gamma: bool) -> DataFrame:
    """PnL = sum(delta * x) + 1/2 sum(gamma * y^2), x/y = scenario shocks mapped to each
    sensitivity's risk factor with the mapping weights (interpolation for FRTB vertices)."""
    use = (F.col("measure") == "DELTA") & (F.col("sens_source") == delta_source)
    if include_gamma:
        use = use | ((F.col("measure") == "GAMMA") & (F.col("sens_source") == gamma_source))
    sens = canon.filter(use).join(valid_positions, "position_id", "left_semi")
    shocks = scen.select("scenario_set", "scenario_id",
                         F.coalesce(F.col("scenario_date").cast("string"), F.lit("")).alias("scenario_date"),
                         "rf_id", F.col("shock_value_canonical").alias("shock"))
    mapped = (
        sens.join(F.broadcast(sens_map), ["sens_source", "sens_rf_key"])
        .join(F.broadcast(shocks), "rf_id")
        .groupBy("position_id", "sens_source", "sens_rf_key", "measure", "value_canonical",
                 "scenario_set", "scenario_id", "scenario_date")
        .agg(F.sum(F.col("weight") * F.col("shock")).alias("mapped_shock"))
    )
    terms = mapped.withColumn(
        "term",
        F.when(F.col("measure") == "DELTA", F.col("value_canonical") * F.col("mapped_shock"))
        .otherwise(0.5 * F.col("value_canonical") * F.col("mapped_shock") ** 2),
    )
    agg = terms.groupBy("position_id", "scenario_set", "scenario_id", "scenario_date").agg(
        F.sum(F.when(F.col("measure") == "DELTA", F.col("term")).otherwise(0.0)).alias("DELTA_PNL"),
        F.sum(F.when(F.col("measure") == "GAMMA", F.col("term")).otherwise(0.0)).alias("GAMMA_PNL"),
    ).withColumn("PNL", F.col("DELTA_PNL") + F.col("GAMMA_PNL"))
    return agg.select(
        "position_id", "scenario_set", "scenario_id", "scenario_date", F.lit("TAYLOR").alias("method"),
        F.expr("stack(3, 'PNL', PNL, 'DELTA_PNL', DELTA_PNL, 'GAMMA_PNL', GAMMA_PNL) as (item, value)"),
    )
