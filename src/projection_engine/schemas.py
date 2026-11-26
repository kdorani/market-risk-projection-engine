"""Source-table contracts: which tables each upstream system provides, schema and keys.

Explicit schemas (never inferSchema) are part of the data contract: a type change upstream
fails loudly at ingestion instead of silently producing wrong PnL.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _schema(*cols: tuple[str, object, bool]) -> StructType:
    return StructType([StructField(n, t, nullable) for n, t, nullable in cols])


S, D, INT, DT = StringType(), DoubleType(), IntegerType(), DateType()


@dataclass(frozen=True)
class SourceTable:
    name: str  # engine staging name (stg_<name>)
    system: str  # POSITION_DATA | SENSITIVITIES_DATA | SHOCKS_DATA | REFERENCE
    source_object: str  # file stem / database table name
    schema: StructType
    primary_key: tuple[str, ...]
    has_business_date: bool = True


SOURCE_TABLES: list[SourceTable] = [
    # ------------------------------------------------------------- position data
    SourceTable("positions", "POSITION_DATA", "positions", _schema(
        ("business_date", DT, False), ("position_id", S, False), ("trade_id", S, False),
        ("book_id", S, False), ("desk_id", S, False), ("product_type", S, False),
        ("instrument_id", S, False), ("currency", S, False), ("notional", D, False),
        ("trade_date", DT, True), ("source_system", S, False),
    ), ("business_date", "position_id")),
    SourceTable("bond_terms", "POSITION_DATA", "bond_terms", _schema(
        ("business_date", DT, False), ("instrument_id", S, False), ("isin", S, True),
        ("issuer", S, True), ("currency", S, False), ("issue_date", DT, False),
        ("maturity_date", DT, False), ("coupon_rate", D, False), ("coupon_frequency", INT, False),
        ("day_count", S, False), ("business_day_convention", S, False), ("calendar_id", S, False),
        ("discount_curve_id", S, False),
    ), ("business_date", "instrument_id")),
    # ------------------------------------------------------------- sensitivities
    SourceTable("fo_sensitivities", "SENSITIVITIES_DATA", "fo_sensitivities", _schema(
        ("business_date", DT, False), ("position_id", S, False), ("trade_id", S, False),
        ("risk_factor_name", S, False), ("sensitivity_type", S, False),
        ("sensitivity_value", D, False), ("unit", S, False), ("pricing_model", S, True),
        ("source_system", S, False),
    ), ("business_date", "position_id", "risk_factor_name", "sensitivity_type")),
    SourceTable("frtb_sensitivities", "SENSITIVITIES_DATA", "frtb_sensitivities", _schema(
        ("business_date", DT, False), ("trade_id", S, False), ("position_id", S, False),
        ("risk_class", S, False), ("risk_type", S, False), ("bucket", S, False),
        ("risk_factor", S, False), ("vertex", S, False), ("sensitivity_type", S, False),
        ("sensitivity_value", D, False), ("unit", S, False), ("reporting_currency", S, False),
        ("source_system", S, False),
    ), ("business_date", "position_id", "risk_class", "risk_type", "bucket", "risk_factor", "vertex")),
    # ------------------------------------------------------------- scenario shocks
    SourceTable("risk_factors", "SHOCKS_DATA", "risk_factors", _schema(
        ("rf_id", S, False), ("risk_class", S, False), ("rf_type", S, False), ("currency", S, False),
        ("curve_id", S, True), ("tenor_label", S, True), ("tenor_years", D, True),
        ("shock_type", S, False), ("shock_unit", S, False), ("description", S, True),
    ), ("rf_id",), has_business_date=False),
    SourceTable("scenario_sets", "SHOCKS_DATA", "scenario_sets", _schema(
        ("business_date", DT, False), ("scenario_set", S, False), ("description", S, True),
        ("window_start", DT, True), ("window_end", DT, True), ("horizon_days", INT, False),
        ("n_scenarios", INT, False), ("data_origin", S, True),
    ), ("business_date", "scenario_set")),
    SourceTable("scenario_shocks", "SHOCKS_DATA", "scenario_shocks", _schema(
        ("business_date", DT, False), ("scenario_set", S, False), ("scenario_id", S, False),
        ("scenario_date", DT, True), ("scenario_name", S, True), ("rf_id", S, False),
        ("shock_value", D, False), ("shock_unit", S, False),
    ), ("business_date", "scenario_set", "scenario_id", "rf_id")),
    # ------------------------------------------------------------- Reference
    SourceTable("yield_curves", "REFERENCE", "yield_curves", _schema(
        ("business_date", DT, False), ("curve_id", S, False), ("currency", S, False),
        ("tenor_label", S, False), ("tenor_years", D, False), ("zero_rate", D, False),
        ("compounding", S, False), ("interpolation", S, False), ("curve_source", S, True),
    ), ("business_date", "curve_id", "tenor_label")),
    SourceTable("holiday_calendars", "REFERENCE", "holiday_calendars", _schema(
        ("calendar_id", S, False), ("holiday_date", DT, False), ("holiday_name", S, True),
    ), ("calendar_id", "holiday_date"), has_business_date=False),
    SourceTable("rf_name_mapping", "REFERENCE", "rf_name_mapping", _schema(
        ("source_system", S, False), ("source_risk_factor", S, False), ("rf_id", S, False),
        ("mapping_status", S, False), ("effective_from", DT, False),
    ), ("source_system", "source_risk_factor"), has_business_date=False),
]

SOURCE_TABLES_BY_NAME = {t.name: t for t in SOURCE_TABLES}
