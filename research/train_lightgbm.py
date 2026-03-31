"""Convenience entrypoint for LightGBM research training."""

from __future__ import annotations

import sys

from research.train_pipeline import build_parser, main as train_main


def main() -> None:
    if "--engine" not in sys.argv:
        sys.argv.extend(["--engine", "lightgbm"])
    train_main()


if __name__ == "__main__":
    main()
