"""YAML load/save + canonical hashing for rule packs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from ai_trading_system.domains.strategy.rule_pack import StrategyRulePack


def load_rule_pack(path: Path | str) -> StrategyRulePack:
    payload = yaml.safe_load(Path(path).read_text()) or {}
    return StrategyRulePack.model_validate(payload)


def save_rule_pack(pack: StrategyRulePack, path: Path | str) -> None:
    payload = pack.model_dump()
    Path(path).write_text(yaml.safe_dump(payload, sort_keys=True))


def rule_pack_hash(pack: StrategyRulePack) -> str:
    """Stable SHA256 of the canonical JSON encoding of the pack."""
    canonical = json.dumps(pack.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
