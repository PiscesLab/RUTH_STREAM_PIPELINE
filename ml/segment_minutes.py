"""Per-segment, per-minute traffic state built from RUTH floating car data (FCD).

This is the single definition of the digital twin's road state. Training,
evaluation and the streaming pipeline all import it, so what a model is trained
on and what it is served cannot drift apart.

A row describes one road segment (node_from -> node_to) during one minute:
how fast traffic moved, how much of it there was, and how much of it stood
still. Minutes in which no vehicle was on the segment have no row: the FCD
says nothing about an empty road, so neither does the state.

Forecast targets are the segment's mean speed HORIZON minutes later. When the
segment is empty at that time the target is missing and the row is not scored.
"""

from datetime import datetime

import h5py
import numpy as np
import pandas as pd

BIN_SECONDS = 60
HORIZONS_MIN = (5, 15)

# A sample below this speed counts as a stopped vehicle (queueing, closure).
STOPPED_BELOW_MPS = 0.5

FCD_FIELDS = ["timestamp", "node_from", "node_to", "segment_length", "vehicle_id", "speed_mps"]
SEGMENT_KEY = ["node_from", "node_to"]


def load_fcd(h5_path):
    """Read the FCD fields the twin uses from a RUTH fcd_history .h5 file."""
    with h5py.File(h5_path, "r") as f:
        ds = f["fcd"]
        return pd.DataFrame({name: ds.fields(name)[:] for name in FCD_FIELDS})


def to_minute(timestamp_s):
    """Absolute minute index of a unix timestamp (or array of them)."""
    return np.asarray(timestamp_s) // BIN_SECONDS


def clock_to_minute(clock):
    """Minute index of a 'YYYY-mm-dd HH:MM:SS' wall-clock time.

    RUTH writes FCD timestamps by interpreting the simulation's departure time
    in the local time zone of the machine it ran on, so wall-clock times such
    as closure windows must be converted the same way to line up.
    """
    return int(datetime.strptime(clock, "%Y-%m-%d %H:%M:%S").timestamp() // BIN_SECONDS)


def minute_to_clock(minute):
    return datetime.fromtimestamp(int(minute) * BIN_SECONDS).strftime("%H:%M")


def segment_minutes(fcd):
    """Aggregate FCD samples into one row per (segment, minute).

    Columns: mean_speed, min_speed (m/s), n_samples, n_vehicles (distinct),
    stopped_share (fraction of samples below STOPPED_BELOW_MPS), segment_length.
    """
    df = fcd.assign(
        minute=to_minute(fcd["timestamp"]),
        stopped=(fcd["speed_mps"] < STOPPED_BELOW_MPS).astype(np.float32),
    )
    grouped = df.groupby(SEGMENT_KEY + ["minute"], sort=True)
    out = grouped.agg(
        mean_speed=("speed_mps", "mean"),
        min_speed=("speed_mps", "min"),
        n_samples=("speed_mps", "size"),
        n_vehicles=("vehicle_id", "nunique"),
        stopped_share=("stopped", "mean"),
        segment_length=("segment_length", "first"),
    )
    return out.reset_index()


def add_targets(state, horizons=HORIZONS_MIN):
    """Add speed_in_{h}m: the segment's mean speed h minutes later (NaN if empty)."""
    lookup = state.set_index(SEGMENT_KEY + ["minute"])["mean_speed"]
    for h in horizons:
        keys = pd.MultiIndex.from_arrays(
            [state["node_from"], state["node_to"], state["minute"] + h]
        )
        state[f"speed_in_{h}m"] = lookup.reindex(keys).to_numpy()
    return state
