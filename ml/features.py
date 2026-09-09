"""
Shared definition of the windowed traffic features.

Both the training scripts and the live pipeline describe a road segment by the
traffic seen in the last WINDOW_SECONDS. This module is the single source of
truth for what that means, so the features a model is trained on and the
features it is served at run time cannot drift apart.

Why a window at all: the earlier implementation averaged over every event a
segment had ever seen, so after a few hundred events a new observation barely
moved the average and the twin stopped reflecting current conditions. Measured
on SanDiegoFCD100.h5 (3.5 h of traffic), predicting the next 5 minutes:

    estimator              speed MAE     congestion label
    cumulative (old)         2.03 m/s          76.5%
    60 s window              1.20 m/s          83.5%

30 s scored marginally better (1.196 m/s / 83.8%) but 30-120 s are effectively
tied; 60 s sits in the middle of that flat region, so it is the least sensitive
to a different sampling rate or vehicle count.
"""

import h5py
import numpy as np
import pandas as pd

# Length of the rolling window used to describe "conditions now".
WINDOW_SECONDS = 60

# How far ahead the future-congestion model predicts.
HORIZON_SECONDS = 300

# Minimum observations in the horizon before a future label is trustworthy.
MIN_HORIZON_SAMPLES = 2

# Speed thresholds (m/s) separating the congestion classes.
HIGH_BELOW = 5.0
MEDIUM_BELOW = 12.0

FEATURE_COLUMNS = [
    "avg_speed",
    "max_speed",
    "min_speed",
    "std_speed",
    "vehicle_count",
    "observation_count",
    "vehicle_type_diversity",
]


def congestion_label(avg_speed):
    """HIGH / MEDIUM / LOW for a given average speed."""
    if avg_speed < HIGH_BELOW:
        return "HIGH"
    if avg_speed < MEDIUM_BELOW:
        return "MEDIUM"
    return "LOW"


def congestion_level(avg_speed):
    """Numeric form of congestion_label: 0=HIGH, 1=MEDIUM, 2=LOW."""
    if avg_speed < HIGH_BELOW:
        return 0
    if avg_speed < MEDIUM_BELOW:
        return 1
    return 2


def summarise_window(speeds, vehicle_types, vehicle_ids=None):
    """Describe one window of observations.

    Uses the population standard deviation (ddof=0) so it matches what the
    live pipeline computes.

    `vehicle_count` counts *distinct vehicles*, while `observation_count`
    counts samples. They are far apart: FCD samples every vehicle every 5 s,
    so one car crossing a long segment yields a dozen readings. Across
    SanDiegoFCD100.h5 a 60 s window holds 7.2 samples but only 1.2 distinct
    vehicles on average, and the two differ in 86% of windows. They also mean
    different things - many samples from one vehicle indicates dwelling
    (a slow vehicle), while many distinct vehicles indicates density - so both
    are kept as separate features.
    """
    n = len(speeds)
    if n == 0:
        return None

    avg = float(np.mean(speeds))
    mean_sq = float(np.mean(np.square(speeds)))
    variance = max(0.0, mean_sq - avg * avg)

    return {
        "avg_speed": avg,
        "max_speed": float(np.max(speeds)),
        "min_speed": float(np.min(speeds)),
        "std_speed": float(np.sqrt(variance)),
        "vehicle_count": int(len(set(vehicle_ids))) if vehicle_ids is not None else int(n),
        "observation_count": int(n),
        "vehicle_type_diversity": int(len(set(vehicle_types))),
    }


def load_fcd_df(h5_path, dataset_key="fcd"):
    """Load an FCD HDF5 file into a DataFrame with a segment_id column."""
    with h5py.File(h5_path, "r") as f:
        if dataset_key not in f:
            raise KeyError(
                f"Dataset key '{dataset_key}' not found. Available: {list(f.keys())}"
            )
        data = f[dataset_key][:]

    df = pd.DataFrame.from_records(data)

    if "vehicle_type" in df.columns:
        df["vehicle_type"] = df["vehicle_type"].apply(
            lambda x: x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)
        )

    df["segment_id"] = df["node_from"].astype(str) + "_" + df["node_to"].astype(str)
    return df


def build_windowed_dataset(
    df,
    window_seconds=WINDOW_SECONDS,
    horizon_seconds=HORIZON_SECONDS,
    min_horizon_samples=MIN_HORIZON_SAMPLES,
):
    """One training row per event, described by the preceding window.

    Each row carries the features a live twin would hold at that moment, plus
    three targets: the current congestion class, the travel time implied by
    current speed, and the congestion class actually observed over the next
    `horizon_seconds` - a measured outcome rather than a rule applied to the
    present values.
    """
    rows = []

    for segment_id, group in df.groupby("segment_id", sort=False):
        group = group.sort_values("timestamp")
        ts = group["timestamp"].to_numpy()
        speeds = group["speed_mps"].to_numpy(dtype=float)
        types = group["vehicle_type"].to_numpy()
        ids = group["vehicle_id"].to_numpy()
        length = float(group["segment_length"].iloc[0])

        for i in range(len(ts)):
            now = ts[i]

            start = np.searchsorted(ts, now - window_seconds, side="right")
            feats = summarise_window(
                speeds[start : i + 1], types[start : i + 1], ids[start : i + 1]
            )
            if feats is None:
                continue

            # what actually happened next on this segment
            fut_start = np.searchsorted(ts, now, side="right")
            fut_end = np.searchsorted(ts, now + horizon_seconds, side="right")
            future = speeds[fut_start:fut_end]
            if len(future) < min_horizon_samples:
                continue

            avg = feats["avg_speed"]
            feats.update(
                {
                    "segment_id": segment_id,
                    "timestamp": now,
                    "segment_length": length,
                    "congestion": congestion_label(avg),
                    "current_congestion": congestion_level(avg),
                    "travel_time": length / avg if avg > 0 else length / 0.1,
                    "future_congestion": congestion_label(float(np.mean(future))),
                }
            )
            rows.append(feats)

    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError(
            "No training rows produced - the data may be too sparse for the "
            f"{horizon_seconds}s horizon."
        )
    return out.fillna(0)
