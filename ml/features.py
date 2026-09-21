"""What a road-segment twin knows at minute t, as model features.

Built on segment_minutes.py (one row per segment per minute with traffic).
Every feature uses only minutes <= t, so it is available live in the pipeline:

  local      - speed now and over the last 5 / 15 minutes, each relative to
               the segment's usual speed; stopped share; vehicle counts; how
               many recent minutes had traffic at all
  downstream - the slowest relative speed and highest stopped share on the
               segments leaving this segment's end node. A blockage ahead is
               what backs traffic up onto this road, so this is the main
               early signal of spillback.
  static     - usual speed, length, time of day

Speeds are expressed as a ratio to the usual speed so a model learns one notion
of "slow" across motorways and side streets. Forecast targets use the same
ratio; predicted speed = predicted ratio x usual speed.
"""

from datetime import datetime

import numpy as np
import pandas as pd

from segment_minutes import BIN_SECONDS, HORIZONS_MIN, SEGMENT_KEY

WINDOWS_MIN = (5, 15)

# Congestion levels from speed relative to usual speed.
HIGH_BELOW = 0.50
MEDIUM_BELOW = 0.75

# Incident alert: the road is HIGH now and most of its samples are stopped.
STOPPED_ALARM_SHARE = 0.5

# Width of a time-of-day slot in the PTDR-style speed profile, matching the
# 15-minute slots RUTH's PTDR uses (672 = 7 days x 96 slots per segment).
PROFILE_SLOT_MIN = 15

FEATURES = [
    "ratio_now", "min_ratio_now", "stopped_share", "n_vehicles", "n_samples",
    "ratio_5m", "ratio_15m", "minutes_seen_5m", "minutes_seen_15m", "vehicles_15m",
    "down_min_ratio_now", "down_max_stopped_now", "down_min_ratio_5m",
    "usual_speed", "segment_length", "minute_of_day",
]

# "local" needs only the twin's own state; "all" also needs the downstream
# segments' state, which in the pipeline means twins messaging each other.
FEATURE_SETS = {
    "all": FEATURES,
    "local": [f for f in FEATURES if not f.startswith("down_")],
}


def congestion_level(ratio):
    """LOW / MEDIUM / HIGH from speed relative to usual speed (array or scalar)."""
    r = np.asarray(ratio, dtype=float)
    return np.where(r < HIGH_BELOW, "HIGH", np.where(r < MEDIUM_BELOW, "MEDIUM", "LOW"))


def minute_of_day(minute):
    """Local clock minute (0-1439) of an absolute minute index, as the FCD timestamps use."""
    offset = datetime.fromtimestamp(int(minute) * BIN_SECONDS).astimezone().utcoffset()
    return (int(minute) + int(offset.total_seconds() // 60)) % 1440


def twin_features(history, usual_speed, segment_length, feature_names):
    """Features for one segment at its latest closed minute, from the twin's own state.

    history: closed minutes of this segment, oldest first, the newest being the
    minute being described, each (minute, mean_speed, min_speed, n_samples,
    n_vehicles, stopped_share). Same definitions as build_features, which the
    models are trained on; downstream features are not available to a single
    twin and are left missing.
    """
    minute, mean_speed, min_speed, n_samples, n_vehicles, stopped = history[-1]
    values = {
        "ratio_now": mean_speed / usual_speed,
        "min_ratio_now": min_speed / usual_speed,
        "stopped_share": stopped,
        "n_vehicles": n_vehicles,
        "n_samples": n_samples,
        "usual_speed": usual_speed,
        "segment_length": segment_length,
        "minute_of_day": minute_of_day(minute),
    }
    for w in WINDOWS_MIN:
        recent = [h for h in history if h[0] > minute - w]
        samples = sum(h[3] for h in recent)
        values[f"ratio_{w}m"] = sum(h[1] * h[3] for h in recent) / samples / usual_speed
        values[f"minutes_seen_{w}m"] = len(recent)
        if w == 15:
            values["vehicles_15m"] = sum(h[4] for h in recent)
    return [values.get(name, np.nan) for name in feature_names]


def usual_speeds(states):
    """Reference speed per segment: median per-minute mean speed across runs.

    Built from training runs only, and shipped with the model so the pipeline
    uses exactly the same reference.
    """
    table = pd.concat(states, ignore_index=True).groupby(SEGMENT_KEY)["mean_speed"].median()
    return table.rename("usual_speed").reset_index()


def slot_of_day(minute, slot_min=PROFILE_SLOT_MIN):
    """Time-of-day slot index of absolute minute indices (array), local clock."""
    minute = np.asarray(minute)
    if not len(minute):
        return minute
    offset = datetime.fromtimestamp(int(minute[0]) * BIN_SECONDS).astimezone().utcoffset()
    return ((minute + int(offset.total_seconds() // 60)) % 1440) // slot_min


def profile_speeds(states, slot_min=PROFILE_SLOT_MIN):
    """PTDR-style speed profile: reference speed per segment per time-of-day slot.

    RUTH's PTDR keeps, for every segment, a speed distribution per 15-minute
    slot of the week, built offline from past runs. This is the same idea on
    the data we have - single mornings, so time of day rather than of week, and
    the sample-weighted mean rather than the full distribution. It is what a
    twin would answer with if it only knew what is usual at this time, and is
    scored as a forecast baseline alongside persistence and usual speed.
    """
    df = pd.concat(states, ignore_index=True)
    df = df.assign(slot=slot_of_day(df["minute"].to_numpy(), slot_min),
                   speed_x_samples=df["mean_speed"] * df["n_samples"])
    g = df.groupby(SEGMENT_KEY + ["slot"])[["speed_x_samples", "n_samples"]].sum()
    profile = (g["speed_x_samples"] / g["n_samples"]).rename("profile_speed").reset_index()
    return profile


def _window_sums(key, minute, values, window):
    """Sum of each column over the trailing `window` minutes of the same segment.

    Rows must be sorted by (segment, minute). Works with gaps: a minute without
    traffic simply contributes nothing.
    """
    relative = minute - minute.min()          # < 10^5 for any single run
    order_key = key.astype(np.int64) * 100_000 + relative
    start = np.searchsorted(order_key, order_key - (window - 1), side="left")
    out = {}
    for name, v in values.items():
        csum = np.concatenate([[0.0], np.cumsum(v, dtype=float)])
        out[name] = csum[np.arange(len(v)) + 1] - csum[start]
    return out


def build_features(state, usual):
    """Add FEATURES (and ratio targets if present) to a segment_minutes table."""
    df = state.merge(usual, on=SEGMENT_KEY, how="left")
    fallback = float(usual["usual_speed"].median())
    df["has_usual"] = df["usual_speed"].notna()
    df["usual_speed"] = df["usual_speed"].fillna(fallback).clip(lower=1.0)

    df = df.sort_values(SEGMENT_KEY + ["minute"], kind="mergesort").reset_index(drop=True)
    seg_code = df.groupby(SEGMENT_KEY, sort=False).ngroup().to_numpy()
    minute = df["minute"].to_numpy()

    df["ratio_now"] = df["mean_speed"] / df["usual_speed"]
    df["min_ratio_now"] = df["min_speed"] / df["usual_speed"]

    base = {
        "speed_x_samples": (df["mean_speed"] * df["n_samples"]).to_numpy(),
        "samples": df["n_samples"].to_numpy(dtype=float),
        "vehicles": df["n_vehicles"].to_numpy(dtype=float),
        "seen": np.ones(len(df)),
    }
    for w in WINDOWS_MIN:
        s = _window_sums(seg_code, minute, base, w)
        df[f"ratio_{w}m"] = s["speed_x_samples"] / s["samples"] / df["usual_speed"]
        df[f"minutes_seen_{w}m"] = s["seen"]
        if w == 15:
            df["vehicles_15m"] = s["vehicles"]

    # downstream: segments whose node_from is this segment's node_to, same minute
    ahead = df.groupby(["node_from", "minute"]).agg(
        down_min_ratio_now=("ratio_now", "min"),
        down_max_stopped_now=("stopped_share", "max"),
        down_min_ratio_5m=("ratio_5m", "min"),
    )
    ahead.index = ahead.index.set_names(["node_to", "minute"])
    df = df.join(ahead, on=["node_to", "minute"])

    # local clock, same convention as the FCD timestamps (see segment_minutes.clock_to_minute)
    if len(df):
        offset_min = datetime.fromtimestamp(int(minute[0]) * BIN_SECONDS).astimezone().utcoffset()
        df["minute_of_day"] = (df["minute"] + int(offset_min.total_seconds() // 60)) % 1440
    else:
        df["minute_of_day"] = []

    for h in HORIZONS_MIN:
        col = f"speed_in_{h}m"
        if col in df:
            df[f"ratio_in_{h}m"] = df[col] / df["usual_speed"]
    return df
