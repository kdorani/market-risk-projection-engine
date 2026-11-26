"""Typed run configuration loaded from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "run_config.yaml"


@dataclass
class SourceConfig:
    system: str
    type: str
    path: Path | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class EngineConfig:
    business_date: date
    warehouse_dir: Path
    output_dir: Path
    sources: dict[str, SourceConfig]
    spark: dict[str, Any]
    scenario_sets: list[str]
    methods: list[str]
    taylor: dict[str, Any]
    grid: dict[str, Any]
    ladder: dict[str, Any]
    risk_measures: dict[str, Any]
    reconciliation: dict[str, Any]
    reporting: dict[str, Any]
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def load(cls, path: str | Path | None = None, root: Path = PROJECT_ROOT) -> EngineConfig:
        path = Path(path) if path else DEFAULT_CONFIG
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        def resolve(p: str) -> Path:
            q = Path(p)
            return q if q.is_absolute() else (root / q)

        sources = {}
        for system, spec in raw["sources"].items():
            spec = dict(spec)
            stype = spec.pop("type")
            src_path = resolve(spec.pop("path")) if "path" in spec else None
            sources[system] = SourceConfig(system, stype, src_path, spec)
        bd = raw["business_date"]
        return cls(
            business_date=bd if isinstance(bd, date) else date.fromisoformat(str(bd)),
            warehouse_dir=resolve(raw["paths"]["warehouse_dir"]),
            output_dir=resolve(raw["paths"]["output_dir"]),
            sources=sources,
            spark=raw.get("spark", {}),
            scenario_sets=list(raw["scenario_sets"]),
            methods=list(raw["methods"]),
            taylor=raw["taylor"],
            grid=raw["grid"],
            ladder=raw["ladder"],
            risk_measures=raw["risk_measures"],
            reconciliation=raw["reconciliation"],
            reporting=raw.get("reporting", {}),
            raw=raw,
        )
