"""Deterministic standard-library serialization for opportunity contracts."""

from __future__ import annotations

import json
import types
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints


T = TypeVar("T")


def to_dict(value: Any) -> Any:
    """Recursively convert a contract value into deterministic JSON data."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        hints = get_type_hints(type(value))
        return {
            item.name: _encode_typed(getattr(value, item.name), hints.get(item.name, Any))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): to_dict(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (tuple, list)):
        return [to_dict(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported opportunity-contract value: {type(value).__name__}")


def from_dict(contract_type: type[T], payload: Mapping[str, Any]) -> T:
    """Strictly reconstruct a dataclass contract from serialized data."""
    if not is_dataclass(contract_type):
        raise TypeError("contract_type must be a dataclass type")
    hints = get_type_hints(contract_type)
    field_names = {item.name for item in fields(contract_type)}
    unknown = set(payload) - field_names
    if unknown:
        raise ValueError(f"unknown fields for {contract_type.__name__}: {sorted(unknown)}")
    kwargs = {
        item.name: _decode(hints.get(item.name, Any), payload[item.name])
        for item in fields(contract_type)
        if item.name in payload
    }
    return contract_type(**kwargs)


def to_json(value: Any) -> str:
    """Serialize a contract deterministically as compact JSON."""
    return json.dumps(to_dict(value), sort_keys=True, separators=(",", ":"))


def _encode_typed(value: Any, expected_type: Any) -> Any:
    if value is None:
        return None
    origin = get_origin(expected_type)
    args = get_args(expected_type)
    if origin in (Union, types.UnionType):
        for option in (item for item in args if item is not type(None)):
            if option is float and isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
        return to_dict(value)
    if expected_type is float and isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if origin is tuple and isinstance(value, (tuple, list)):
        item_type = args[0] if args else Any
        return [_encode_typed(item, item_type) for item in value]
    if origin in (dict, Mapping) or (origin is not None and issubclass(origin, Mapping)):
        value_type = args[1] if len(args) == 2 else Any
        return {str(key): _encode_typed(value[key], value_type) for key in sorted(value, key=str)}
    return to_dict(value)


def _decode(expected_type: Any, value: Any) -> Any:
    if expected_type is Any:
        return value
    origin = get_origin(expected_type)
    args = get_args(expected_type)
    if origin in (Union, types.UnionType):
        if value is None and type(None) in args:
            return None
        failures: list[Exception] = []
        for option in (item for item in args if item is not type(None)):
            try:
                return _decode(option, value)
            except (TypeError, ValueError) as exc:
                failures.append(exc)
        raise ValueError(f"value {value!r} does not match {expected_type!r}") from failures[-1]
    if isinstance(expected_type, type) and issubclass(expected_type, Enum):
        return expected_type(value)
    if expected_type is datetime:
        if not isinstance(value, str):
            raise TypeError("datetime values must be ISO-8601 strings")
        return datetime.fromisoformat(value)
    if expected_type is date:
        if not isinstance(value, str):
            raise TypeError("date values must be ISO-8601 strings")
        return date.fromisoformat(value)
    if isinstance(expected_type, type) and is_dataclass(expected_type):
        if not isinstance(value, Mapping):
            raise TypeError(f"{expected_type.__name__} must be an object")
        return from_dict(expected_type, value)
    if origin is tuple:
        if not isinstance(value, (list, tuple)):
            raise TypeError("tuple values must be arrays")
        item_type = args[0] if args else Any
        return tuple(_decode(item_type, item) for item in value)
    if origin in (dict, Mapping) or (origin is not None and issubclass(origin, Mapping)):
        if not isinstance(value, Mapping):
            raise TypeError("mapping values must be objects")
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        return {_decode(key_type, key): _decode(value_type, item) for key, item in value.items()}
    if expected_type in (str, int, float, bool):
        if not isinstance(value, expected_type):
            if expected_type is float and isinstance(value, int) and not isinstance(value, bool):
                return float(value)
            raise TypeError(f"expected {expected_type.__name__}, got {type(value).__name__}")
        return value
    return value
