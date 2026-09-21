"""Locate the central San Diego runs and turn them into model-ready tables.

A run is (level, seed, condition): e.g. 10k vehicles, seed 3, with incidents.
Seeds split the data so a model is always scored on mornings it never saw:

    SPLITS = train: seeds 1-2 | validation: seed 3 | test: seed 4

Per-run segment-minute tables are cached (ml/cache/) because aggregating a
10k-vehicle FCD file takes about a minute.

Incident labels: a segment-minute is incident-affected when the segment is a
closed road or up to AFFECTED_HOPS roads upstream of one, from the closure
start until AFTER_MIN minutes after it reopens.
"""

import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

from segment_minutes import add_targets, clock_to_minute, load_fcd, segment_minutes

RUNS_ROOT = "/users/Dinisha/RUTH/benchmarks/san-diego-central/runs"
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")

LEVELS = ("1k", "5k", "10k", "100k")
CONDITIONS = ("control", "incidents")
SPLITS = {"train": (1, 2), "validation": (3,), "test": (4,)}

AFFECTED_HOPS = 2
AFTER_MIN = 15


@dataclass(frozen=True)
class Run:
    level: str
    seed: int
    condition: str

    @property
    def tag(self):
        return f"{self.level}S{self.seed}"

    @property
    def folder(self):
        return os.path.join(RUNS_ROOT, f"{self.level}_seed{self.seed}")

    @property
    def fcd_path(self):
        return os.path.join(self.folder, self.condition,
                            f"SanDiegoCentralFCD{self.tag}{self.condition.capitalize()}.h5")

    @property
    def incidents_path(self):
        return os.path.join(self.folder, f"SanDiegoCentralIncidents{self.tag}.csv")

    def __str__(self):
        return f"{self.tag}-{self.condition}"


def runs(split, levels=LEVELS, conditions=CONDITIONS):
    return [Run(lvl, s, c) for lvl in levels for s in SPLITS[split] for c in conditions
            if os.path.exists(Run(lvl, s, c).fcd_path)]


def state(run):
    """Segment-minute table with forecast targets for one run (cached)."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{run}.pkl")
    if os.path.exists(path) and os.path.getmtime(path) > os.path.getmtime(run.fcd_path):
        return pd.read_pickle(path)
    table = add_targets(segment_minutes(load_fcd(run.fcd_path)))
    table.to_pickle(path)
    return table


def incidents(run):
    """The run's closures with start/end minute indices (same file for both conditions)."""
    inc = pd.read_csv(run.incidents_path, sep=";")
    inc["start_minute"] = inc["timestamp_from"].map(clock_to_minute)
    inc["end_minute"] = inc["timestamp_to"].map(clock_to_minute)
    return inc


def affected_mask(df, inc):
    """True for segment-minutes on or up to AFFECTED_HOPS roads upstream of a closure, during it."""
    segments = df[["node_from", "node_to"]].drop_duplicates()
    mask = np.zeros(len(df), dtype=bool)
    for c in inc.itertuples():
        hit = {(c.node_from, c.node_to)}
        frontier = {c.node_from}
        for _ in range(AFFECTED_HOPS):
            up = segments[segments["node_to"].isin(frontier)]
            hit |= set(map(tuple, up.to_numpy()))
            frontier = set(up["node_from"])
        in_time = (df["minute"] >= c.start_minute) & (df["minute"] <= c.end_minute + AFTER_MIN)
        on_road = pd.MultiIndex.from_frame(df[["node_from", "node_to"]]).isin(list(hit))
        mask |= in_time.to_numpy() & on_road
    return mask
