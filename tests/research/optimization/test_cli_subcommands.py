"""Unit tests for the `init` and `validate` subcommands + name-based baseline
resolution. These tests are intentionally engine-free: they cover the cheap
schema/path checks. The `--with-backtest` flow is covered transitively by
``test_runner_integration.py`` (which already exercises the engine).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ai_trading_system.research.optimization.cli import main as cli_main
from ai_trading_system.research.optimization.recipe import (
    load_recipe,
    resolve_baseline_path,
)


# ---------------------------------------------------------------------------
# resolve_baseline_path
# ---------------------------------------------------------------------------


def test_resolve_baseline_path_bare_name(tmp_path: Path) -> None:
    got = resolve_baseline_path("momentum_breakout_v1", project_root=tmp_path)
    assert got == tmp_path / "config" / "strategies" / "momentum_breakout_v1.yaml"


def test_resolve_baseline_path_relative_path(tmp_path: Path) -> None:
    got = resolve_baseline_path(
        "config/strategies/momentum_breakout_v1.yaml", project_root=tmp_path
    )
    assert got == tmp_path / "config" / "strategies" / "momentum_breakout_v1.yaml"


def test_resolve_baseline_path_absolute_path(tmp_path: Path) -> None:
    abs_path = tmp_path / "elsewhere" / "pack.yaml"
    got = resolve_baseline_path(str(abs_path), project_root=tmp_path / "other_root")
    assert got == abs_path  # absolute paths pass through unchanged


def test_resolve_baseline_path_yml_extension(tmp_path: Path) -> None:
    got = resolve_baseline_path("pack.yml", project_root=tmp_path)
    # ``.yml`` treated as literal — relative to project_root
    assert got == tmp_path / "pack.yml"


# ---------------------------------------------------------------------------
# init subcommand
# ---------------------------------------------------------------------------


def _argv_for(*parts: str) -> list[str]:
    return list(parts)


def test_init_creates_both_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli_main(_argv_for("init", "demo_strategy", "--project-root", str(tmp_path)))
    assert rc == 0
    rule_pack = tmp_path / "config" / "strategies" / "demo_strategy_v1.yaml"
    recipe = tmp_path / "config" / "strategies" / "recipes" / "demo_strategy.yaml"
    assert rule_pack.exists()
    assert recipe.exists()
    captured = capsys.readouterr().out
    assert "Next steps:" in captured
    assert "ai-trading-optimize validate demo_strategy" in captured


def test_init_refuses_existing_without_force(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # Seed an existing rule pack file.
    rule_pack_path = tmp_path / "config" / "strategies" / "demo_v1.yaml"
    rule_pack_path.parent.mkdir(parents=True, exist_ok=True)
    rule_pack_path.write_text("preexisting: true\n")

    rc = cli_main(_argv_for("init", "demo", "--project-root", str(tmp_path)))
    assert rc == 2
    out = capsys.readouterr().out
    assert "already exist" in out
    assert "--force" in out
    # Original file untouched.
    assert rule_pack_path.read_text() == "preexisting: true\n"


def test_init_overwrites_with_force(tmp_path: Path) -> None:
    rule_pack_path = tmp_path / "config" / "strategies" / "demo_v1.yaml"
    rule_pack_path.parent.mkdir(parents=True, exist_ok=True)
    rule_pack_path.write_text("preexisting: true\n")

    rc = cli_main(_argv_for("init", "demo", "--project-root", str(tmp_path), "--force"))
    assert rc == 0
    # Should now be the rendered template.
    assert "strategy_id: demo" in rule_pack_path.read_text()


def test_init_renders_loadable_recipe_and_pack(tmp_path: Path) -> None:
    """The scaffolded files must pass the same Pydantic validation the runner uses,
    and the recipe's bare ``baseline_pack_path`` must resolve back to the
    scaffolded ``_v1`` rule pack file."""
    rc = cli_main(_argv_for("init", "smoke_demo", "--project-root", str(tmp_path)))
    assert rc == 0
    recipe_path = tmp_path / "config" / "strategies" / "recipes" / "smoke_demo.yaml"
    recipe = load_recipe(recipe_path)
    assert recipe.name == "smoke_demo"
    assert recipe.strategy_id == "smoke_demo"
    # init writes ``baseline_pack_path: smoke_demo_v1`` (bare name pointing at
    # the scaffolded ``smoke_demo_v1.yaml``).
    assert recipe.baseline_pack_path == "smoke_demo_v1"
    baseline_full = resolve_baseline_path(recipe.baseline_pack_path, project_root=tmp_path)
    assert baseline_full.exists()
    assert baseline_full.name == "smoke_demo_v1.yaml"


# ---------------------------------------------------------------------------
# validate subcommand
# ---------------------------------------------------------------------------


def test_validate_happy_path_after_init(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cli_main(_argv_for("init", "vdemo", "--project-root", str(tmp_path)))
    capsys.readouterr()  # drain init output

    rc = cli_main(_argv_for("validate", "vdemo", "--project-root", str(tmp_path)))
    assert rc == 0
    out = capsys.readouterr().out
    assert "OK:" in out
    assert "baseline pack:" in out
    assert "strategy_id:" in out


def test_validate_missing_recipe(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli_main(_argv_for("validate", "does_not_exist", "--project-root", str(tmp_path)))
    assert rc == 2
    out = capsys.readouterr().out
    assert "recipe not found" in out


def test_validate_missing_baseline_pack(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # Scaffold then delete the rule pack so the recipe is dangling.
    cli_main(_argv_for("init", "dangling", "--project-root", str(tmp_path)))
    rule_pack = tmp_path / "config" / "strategies" / "dangling_v1.yaml"
    rule_pack.unlink()
    capsys.readouterr()

    rc = cli_main(_argv_for("validate", "dangling", "--project-root", str(tmp_path)))
    assert rc == 1
    out = capsys.readouterr().out
    assert "baseline pack not found" in out


def test_validate_broken_recipe_schema(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    recipe_path = tmp_path / "config" / "strategies" / "recipes" / "broken.yaml"
    recipe_path.parent.mkdir(parents=True, exist_ok=True)
    # Missing required ``name`` and other fields.
    recipe_path.write_text("strategy_id: x\n")

    rc = cli_main(_argv_for("validate", "broken", "--project-root", str(tmp_path)))
    assert rc == 1
    out = capsys.readouterr().out
    assert "recipe schema invalid" in out


# ---------------------------------------------------------------------------
# Dispatch / backwards compat
# ---------------------------------------------------------------------------


def test_unknown_subcommand_falls_through_to_legacy_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """argv[0] that is NOT init/validate/run must hit the legacy --recipe parser.

    We assert by capturing the SystemExit from argparse when --recipe is missing
    — that proves dispatch routed to the run parser, not to a phantom subcommand.
    """
    with pytest.raises(SystemExit) as excinfo:
        cli_main(["not_a_subcommand"])
    # argparse exits with code 2 on missing required argument.
    assert excinfo.value.code == 2


def test_legacy_recipe_flag_dispatches_to_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`ai-trading-optimize --recipe foo` (no subcommand) must hit run."""
    with pytest.raises(SystemExit) as excinfo:
        # No recipe file exists — argparse error proves we entered the run parser.
        cli_main(["--recipe", "nonexistent_recipe", "--project-root", str(tmp_path)])
    assert excinfo.value.code == 2
