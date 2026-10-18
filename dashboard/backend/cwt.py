"""The pipeline stores only the scalar CRI, not the time-frequency scalogram — so we recompute it here from the 1-min median series (the same input the pipeline uses), with the SAME complex-Morlet wavelet the Java `MorletCwt` uses. """
from datetime import datetime, timezone
import numpy as np
import pywt

DT = 1.0 / 60.0
WAVELET = "cmor2.0-0.9549"  # complex Morlet ~ omega0=6 (matches the Java MorletCwt)
BAND = (20.0, 28.0)

def scalogram(times, values, min_period=0.5, max_period=48.0, voices=8, max_cols=900):
    """Return {periods, times, power[period][time], band}. Time is downsampled to <= max_cols columns
    so the JSON stays small; power is |W|^2. Gaps are filled with the window mean (as the pipeline does)."""
    if not times or len(times) < 60:
        return {"periods": [], "times": [], "power": [], "band": list(BAND)}

    secs = np.array([t.timestamp() for t in times], dtype=float)
    vals = np.array(values, dtype=float)
    t0 = secs.min()
    minute_idx = np.rint((secs - t0) / 60.0).astype(int)
    n = int(minute_idx.max()) + 1

    grid = np.full(n, np.nan)
    grid[minute_idx] = vals
    mean = np.nanmean(grid)
    grid = np.where(np.isnan(grid), mean, grid) - mean 

    n_scales = int(np.floor(voices * np.log2(max_period / min_period))) + 1
    periods = min_period * 2.0 ** (np.arange(n_scales) / voices)
    central = pywt.central_frequency(WAVELET)
    scales = periods * central / DT
    coef, _ = pywt.cwt(grid, scales, WAVELET, sampling_period=DT)
    power = np.abs(coef) ** 2 

    step = max(1, int(np.ceil(n / max_cols)))
    power = power[:, ::step]
    col_secs = t0 + np.arange(0, n, step) * 60.0
    times_iso = [datetime.fromtimestamp(s, tz=timezone.utc).isoformat() for s in col_secs]

    col_hours = (col_secs - t0) / 3600.0
    span = col_hours[-1] if len(col_hours) else 0.0
    edge_dist = np.minimum(col_hours, span - col_hours)
    coi = edge_dist / (np.sqrt(2.0) * central)

    return {
        "periods": [round(p, 4) for p in periods.tolist()],
        "times": times_iso,
        "power": power.tolist(),
        "band": list(BAND),
        "coi": [round(c, 4) for c in coi.tolist()],
    }