from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_collectors_top_level_modules_are_compatibility_shims() -> None:
    collectors_dir = _repo_root() / "collectors"
    for path in sorted(collectors_dir.glob("*.py")):
        if path.name == "__init__.py":
            continue
        source = path.read_text(encoding="utf-8")
        assert "from ai_trading_system." in source, path.name
        assert "_sys.modules[__name__]" in source, path.name


def test_phase6_analytics_shims_keep_legacy_import_paths() -> None:
    import analytics.ranker as legacy_ranker
    import analytics.regime_detector as legacy_regime_detector
    import analytics.screener as legacy_screener

    from ai_trading_system.domains.ranking import ranker as canonical_ranker
    from ai_trading_system.domains.ranking import regime_detector as canonical_regime_detector
    from ai_trading_system.domains.ranking import screener as canonical_screener

    assert legacy_ranker is canonical_ranker
    assert legacy_regime_detector is canonical_regime_detector
    assert legacy_screener is canonical_screener

