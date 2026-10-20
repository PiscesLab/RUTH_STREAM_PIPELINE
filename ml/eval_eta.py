"""Score the route arrival-time estimates against what actually happened.

The twins publish an estimate of how long a vehicle will take to cross the next
few segments. The routes table says when the vehicle really got there, so the
estimate can be scored directly, and against the alternatives a navigation
system would use instead:

    twin forecasts    what the segment twins answered, from live state
    usual speed       each segment's reference speed, one number per segment
    historical profile  each segment's usual speed in the 15-minute slot the
                      vehicle is expected to arrive in - the information
                      time-dependent routing uses

This is the same comparison as the per-segment one, at the level a driver
actually cares about: a whole stretch of road rather than one segment.

    python3 ml/eval_eta.py --eta /tmp/eta_out.jsonl --routes ml/models/routes_1kS3.json
"""

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dataset  # noqa: E402
from features import profile_speeds, slot_of_day  # noqa: E402

MIN_SPEED = 1.0          # m/s, so a stopped segment gives a finite estimate


def load_usual(path):
    usual = {}
    with open(path) as f:
        next(f)
        for line in f:
            node_from, node_to, speed = line.strip().split(",")
            usual[f"{node_from}_{node_to}"] = float(speed)
    return usual


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--eta", required=True, help="JSONL of published ETA records")
    p.add_argument("--routes", required=True)
    p.add_argument("--usual", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                   "models", "usual_speed.csv"))
    p.add_argument("--level", default="1k", help="training level the profile is built from")
    p.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                 "results", "eta.json"))
    args = p.parse_args()

    records = [json.loads(line) for line in open(args.eta) if line.strip()]
    routes = {int(k): v for k, v in json.load(open(args.routes)).items()}
    usual = load_usual(args.usual)
    fallback = float(np.median(list(usual.values())))

    train_runs = dataset.runs("train", levels=[args.level], conditions=("control",))
    profile = profile_speeds([dataset.state(r) for r in train_runs])
    profile_lookup = {(int(r.node_from), int(r.node_to), int(r.slot)): float(r.profile_speed)
                      for r in profile.itertuples(index=False)}

    rows = []
    for record in records:
        route = routes.get(record["vehicle_id"])
        if not route:
            continue
        index = record["from_route_index"]
        ahead = route[index + 1: index + 1 + record["segments_ahead"]]
        if len(ahead) < record["segments_ahead"]:
            continue
        # what really happened: from entering the first segment ahead to
        # entering the one after the last, or leaving the last if the route ends
        start = ahead[0][2]
        after = index + 1 + record["segments_ahead"]
        actual = (route[after][2] - start) if after < len(route) else (ahead[-1][3] - start)
        if actual <= 0:
            continue

        twin = record["eta_s"]
        usual_eta = sum(length / max(usual.get(seg, fallback), MIN_SPEED)
                        for seg, length, *_ in ahead)

        # the profile is asked about the slot the vehicle is expected to reach
        # each segment in, which is what time-dependent routing does
        profile_eta, clock = 0.0, start
        for seg, length, *_ in ahead:
            node_from, node_to = (int(x) for x in seg.split("_"))
            slot = int(slot_of_day(np.array([clock // 60]))[0])
            speed = profile_lookup.get((node_from, node_to, slot),
                                       usual.get(seg, fallback))
            seconds = length / max(speed, MIN_SPEED)
            profile_eta += seconds
            clock += seconds

        rows.append((actual, twin, usual_eta, profile_eta))

    if not rows:
        print("no ETA records could be matched to routes")
        return 1

    actual = np.array([r[0] for r in rows], dtype=float)
    results = {}
    for i, name in enumerate(["twin forecasts", "usual speed", "historical profile"], start=1):
        estimate = np.array([r[i] for r in rows], dtype=float)
        error = estimate - actual
        results[name] = {
            "mae_s": round(float(np.abs(error).mean()), 1),
            "median_abs_error_s": round(float(np.median(np.abs(error))), 1),
            "bias_s": round(float(error.mean()), 1),
            "within_20pct": round(float((np.abs(error) <= 0.2 * actual).mean()), 3),
        }

    summary = {
        "records_scored": len(rows),
        "segments_per_estimate": records[0]["segments_ahead"],
        "actual_duration_s": {"median": round(float(np.median(actual)), 1),
                              "mean": round(float(actual.mean()), 1)},
        "estimators": results,
    }
    print(json.dumps(summary, indent=2))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(summary, f, indent=2)
    print("raw:", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
