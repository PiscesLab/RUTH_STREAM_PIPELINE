"""Per-vehicle routes, reconstructed from floating car data.

Route arrival-time estimates need two things that the event stream alone does
not carry: which segments a vehicle still has ahead of it, and when it really
arrived, to score the estimate against. Both are recoverable from the FCD
after the fact, because the records are ordered and carry the vehicle id: the
sequence of segments a vehicle appears on is the route it drove, and the first
timestamp on each segment is when it got there.

The table is built offline and shipped beside the models, the same way
usual_speed.csv is, so the twins can look a route up without computing it.

    python3 ml/routes.py --h5 <run>.h5 --out ml/models/routes_1kS3.json
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from segment_minutes import load_fcd  # noqa: E402

# A vehicle can pass the same segment twice (a loop, a return trip). A gap
# larger than this between its records on one segment means it left and came
# back rather than lingered, so the visits are kept separate.
REVISIT_GAP_S = 60


def build_routes(h5_path, min_segments=3):
    """vehicle id -> the segments it drove, in order, with arrival times.

    Each entry is [segment_id, length_m, arrived_at, left_at]. A route with
    fewer than min_segments segments is dropped: there is nothing to estimate.
    """
    fcd = load_fcd(h5_path).sort_values(["vehicle_id", "timestamp"])
    fcd["segment_id"] = (fcd["node_from"].astype(str) + "_" + fcd["node_to"].astype(str))

    # a new visit whenever the vehicle changes segment, or returns to one after
    # a gap longer than it could have spent sitting there
    same_vehicle = fcd["vehicle_id"].eq(fcd["vehicle_id"].shift())
    same_segment = fcd["segment_id"].eq(fcd["segment_id"].shift())
    gap = fcd["timestamp"].diff()
    new_visit = ~(same_vehicle & same_segment & (gap <= REVISIT_GAP_S))
    fcd["visit"] = new_visit.cumsum()

    visits = (fcd.groupby("visit")
              .agg(vehicle_id=("vehicle_id", "first"),
                   segment_id=("segment_id", "first"),
                   length=("segment_length", "first"),
                   arrived=("timestamp", "min"),
                   left=("timestamp", "max"))
              .reset_index(drop=True)
              .sort_values(["vehicle_id", "arrived"]))

    routes = {}
    for vehicle_id, group in visits.groupby("vehicle_id", sort=False):
        if len(group) < min_segments:
            continue
        routes[int(vehicle_id)] = [
            [row.segment_id, float(row.length), int(row.arrived), int(row.left)]
            for row in group.itertuples(index=False)
        ]
    return routes


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--h5", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--min-segments", type=int, default=3)
    args = p.parse_args()

    routes = build_routes(args.h5, args.min_segments)
    lengths = [len(r) for r in routes.values()]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(routes, f)

    print(f"{len(routes):,} routes written to {args.out}")
    if lengths:
        print(f"segments per route: median {int(np.median(lengths))}, "
              f"max {max(lengths)}, total {sum(lengths):,}")
        spans = [r[-1][3] - r[0][2] for r in routes.values()]
        print(f"route duration: median {int(np.median(spans))}s, max {max(spans)}s")


if __name__ == "__main__":
    main()
