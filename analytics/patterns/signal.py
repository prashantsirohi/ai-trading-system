"""Signal processing helpers for pattern detection."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

_SCIPY_FIND_PEAKS = None
_SCIPY_FIND_PEAKS_LOOKED_UP = False
_KERNEL_REG_CLS = None
_KERNEL_REG_LOOKED_UP = False


@dataclass(frozen=True)
class LocalExtrema:
    index: int
    kind: str
    value: float


def _get_scipy_find_peaks():
    global _SCIPY_FIND_PEAKS, _SCIPY_FIND_PEAKS_LOOKED_UP
    if _SCIPY_FIND_PEAKS_LOOKED_UP:
        return _SCIPY_FIND_PEAKS
    _SCIPY_FIND_PEAKS_LOOKED_UP = True
    try:  # pragma: no cover - exercised when optional deps are installed
        from scipy.signal import find_peaks as _find_peaks
    except ImportError:  # pragma: no cover - fallback path tested instead
        _SCIPY_FIND_PEAKS = None
    else:
        _SCIPY_FIND_PEAKS = _find_peaks
    return _SCIPY_FIND_PEAKS


def _get_kernel_reg_cls():
    global _KERNEL_REG_CLS, _KERNEL_REG_LOOKED_UP
    if _KERNEL_REG_LOOKED_UP:
        return _KERNEL_REG_CLS
    _KERNEL_REG_LOOKED_UP = True
    try:  # pragma: no cover - exercised when optional deps are installed
        from statsmodels.nonparametric.kernel_regression import KernelReg as _KernelReg
    except ImportError:  # pragma: no cover - fallback path tested instead
        _KERNEL_REG_CLS = None
    else:
        _KERNEL_REG_CLS = _KernelReg
    return _KERNEL_REG_CLS


def kernel_smooth(
    prices: pd.Series,
    bandwidth: float = 3.0,
    method: str = "auto",
) -> pd.Series:
    """Smooth a price series with either kernel regression or a faster rolling approximation."""

    series = pd.Series(prices, copy=True).astype(float)
    if len(series) <= 3:
        return series
    normalized_method = str(method or "auto").strip().lower()
    if normalized_method not in {"auto", "kernel", "rolling"}:
        normalized_method = "auto"
    if normalized_method == "rolling":
        window = max(5, int(round(bandwidth * 3)) | 1)
        return series.rolling(window=window, center=True, min_periods=1).mean()
    kernel_reg_cls = _get_kernel_reg_cls()
    if kernel_reg_cls is None:
        window = max(3, int(round(bandwidth * 2)) | 1)
        return series.rolling(window=window, center=True, min_periods=1).mean()
    x = np.arange(len(series), dtype=float)
    kr = kernel_reg_cls(endog=series.to_numpy(), exog=x, var_type="c", bw=[float(bandwidth)])
    smoothed, _ = kr.fit(x)
    return pd.Series(smoothed, index=series.index, dtype=float)


def _fallback_find_peaks(values: np.ndarray, prominence: float) -> np.ndarray:
    """Simple local extrema fallback when scipy is unavailable."""

    peaks: list[int] = []
    for idx in range(1, len(values) - 1):
        center = values[idx]
        if center > values[idx - 1] and center > values[idx + 1]:
            local_prominence = center - max(values[idx - 1], values[idx + 1])
            if local_prominence >= prominence:
                peaks.append(idx)
    return np.asarray(peaks, dtype=int)


def find_local_extrema(smoothed_prices: pd.Series, prominence: float = 0.02) -> list[LocalExtrema]:
    """Return sorted local extrema from a smoothed price series."""

    series = pd.Series(smoothed_prices, copy=False).astype(float)
    if len(series) < 3:
        return []
    values = series.to_numpy()
    price_range = float(np.nanmax(values) - np.nanmin(values))
    abs_prominence = float(prominence if prominence >= 1 else max(price_range * prominence, 1e-6))
    scipy_find_peaks = _get_scipy_find_peaks()
    if scipy_find_peaks is None:
        peak_idx = _fallback_find_peaks(values, abs_prominence)
        trough_idx = _fallback_find_peaks(-values, abs_prominence)
    else:  # pragma: no cover - trivial wrapper around dependency
        peak_idx, _ = scipy_find_peaks(values, prominence=abs_prominence)
        trough_idx, _ = scipy_find_peaks(-values, prominence=abs_prominence)
    extrema = [LocalExtrema(int(idx), "peak", float(values[idx])) for idx in peak_idx]
    extrema.extend(LocalExtrema(int(idx), "trough", float(values[idx])) for idx in trough_idx)
    return sorted(extrema, key=lambda item: item.index)
