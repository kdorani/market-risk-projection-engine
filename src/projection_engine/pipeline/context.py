"""Run context, warehouse writer and step reporter shared by all pipeline steps."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import EngineConfig


class Warehouse:
    """Engine-owned Parquet tables. Writes then re-reads, which materialises each step
    (no recomputation of upstream UDFs) and leaves an auditable table per step."""

    def __init__(self, spark: SparkSession, root: Path, business_date: date, run_id: str):
        self.spark, self.root, self.business_date, self.run_id = spark, root, business_date, run_id
        self.row_counts: dict[str, int] = {}

    def write(self, name: str, df: DataFrame) -> DataFrame:
        path = str(self.root / name)
        if "run_id" not in df.columns:
            df = df.withColumn("run_id", F.lit(self.run_id))
        if "business_date" in df.columns:
            df.write.mode("overwrite").partitionBy("business_date").parquet(path)
            out = self.spark.read.parquet(path).filter(F.col("business_date") == F.lit(self.business_date))
        else:
            df.write.mode("overwrite").parquet(path)
            out = self.spark.read.parquet(path)
        self.row_counts[name] = out.count()
        return out


class StepReporter:
    """Prints each step's output and saves it as CSV plus a Markdown run report."""

    def __init__(self, output_dir: Path, max_rows_console: int = 25, quiet: bool = False):
        self.steps_dir = output_dir / "steps"
        self.steps_dir.mkdir(parents=True, exist_ok=True)
        for old in self.steps_dir.glob("*.csv"):
            old.unlink()
        self.output_dir = output_dir
        self.max_rows = max_rows_console
        self.quiet = quiet
        self.sections: list[str] = []
        self.timings: dict[str, float] = {}
        self._t0 = time.perf_counter()

    def step(self, number: str, title: str, description: str,
             tables: dict[str, pd.DataFrame], max_rows: int | None = None) -> None:
        elapsed = time.perf_counter() - self._t0
        self.timings[f"{number} {title}"] = round(elapsed, 2)
        self._t0 = time.perf_counter()
        rows = max_rows or self.max_rows
        md = [f"## Step {number}: {title}", "", description, ""]
        if not self.quiet:
            print("\n" + "=" * 100 + f"\nSTEP {number}: {title}\n" + "=" * 100)
            print(description)
        for name, pdf in tables.items():
            file = self.steps_dir / f"{number}_{name}.csv"
            pdf.to_csv(file, index=False)
            shown = _round_frame(pdf.head(rows))
            if not self.quiet:
                print(f"\n--- {name} ({len(pdf)} rows; saved to {_display_path(file)})")
                with pd.option_context("display.width", 220, "display.max_columns", 40,
                                       "display.max_colwidth", 60):
                    print(shown.to_string(index=False))
            md += [f"**{name}** — {len(pdf)} rows (`{file.name}`)", "", _markdown_table(shown), ""]
            if len(pdf) > rows:
                md += [f"_showing first {rows} of {len(pdf)} rows_", ""]
        self.sections.append("\n".join(md))

    def write_report(self, header: str) -> Path:
        path = self.output_dir / "run_report.md"
        path.write_text(header + "\n\n" + "\n\n".join(self.sections) + "\n", encoding="utf-8")
        return path


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(path)


def _round_frame(pdf: pd.DataFrame) -> pd.DataFrame:
    """Round floats for display by column magnitude (keeps small gammas/weights readable)."""
    out = pdf.drop(columns=["business_date", "run_id"], errors="ignore").copy()
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]):
            m = out[c].abs().max()
            out[c] = out[c].round(2 if m >= 100 else 4 if m >= 1 else 6)
    return out


def _markdown_table(pdf: pd.DataFrame) -> str:
    if pdf.empty:
        return "_(empty)_"

    def fmt(v):
        if isinstance(v, (float, np.floating)):
            if np.isnan(v):
                return ""
            return f"{v:,.6f}".rstrip("0").rstrip(".") if abs(v) < 100 else f"{v:,.2f}"
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        return str(v).replace("|", "\\|")

    header = "| " + " | ".join(map(str, pdf.columns)) + " |"
    sep = "| " + " | ".join("---" for _ in pdf.columns) + " |"
    body = ["| " + " | ".join(fmt(v) for v in row) + " |" for row in pdf.itertuples(index=False)]
    return "\n".join([header, sep, *body])


@dataclass
class RunContext:
    cfg: EngineConfig
    spark: SparkSession
    warehouse: Warehouse
    reporter: StepReporter
    run_id: str
    started_at: str
    source_checksums: dict[str, str] = field(default_factory=dict)

    @classmethod
    def create(cls, cfg: EngineConfig, spark: SparkSession, quiet: bool = False) -> RunContext:
        now = datetime.now(timezone.utc)
        run_id = f"{cfg.business_date.isoformat()}_{now:%H%M%S}_{uuid.uuid4().hex[:6]}"
        return cls(
            cfg=cfg,
            spark=spark,
            warehouse=Warehouse(spark, cfg.warehouse_dir, cfg.business_date, run_id),
            reporter=StepReporter(cfg.output_dir, cfg.reporting.get("max_rows_console", 25), quiet),
            run_id=run_id,
            started_at=now.isoformat(timespec="seconds"),
        )
