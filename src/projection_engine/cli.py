"""Command-line entry point.

    python -m projection_engine                       # default config/run_config.yaml
    python -m projection_engine --config my.yaml --quiet
    projection-engine --config config/run_config.yaml # after `pip install -e .`
"""

from __future__ import annotations

import argparse
import sys

from .pipeline.quality import DataQualityError
from .pipeline.runner import run_engine


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Market risk projection engine (PnL vectors, VaR, SVaR)")
    parser.add_argument("--config", default=None, help="path to run config YAML")
    parser.add_argument("--quiet", action="store_true", help="do not print step outputs")
    args = parser.parse_args(argv)
    try:
        run_engine(args.config, quiet=args.quiet)
    except DataQualityError as exc:
        print(f"\nRUN STOPPED - data quality:\n{exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
