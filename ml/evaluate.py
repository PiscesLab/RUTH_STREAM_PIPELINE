#!/usr/bin/env python3
"""Evaluate the speed forecasts and everything derived from them.

Scored on seeds the models never saw (validation = seed 3 for tuning,
test = seed 4, used once for the final numbers), separately per traffic level:

  1. speed        - forecast error (m/s) vs "speed stays the same" (persistence)
                    and "usual speed", on all roads and on incident-affected roads
  2. congestion   - is the road HIGH (below 50% of usual speed) in h minutes?
                    precision / recall / F1 for the forecast vs persistence
  3. travel time  - error of length / speed for crossing the road in h minutes
  4. early warning- when a road near a closure turns HIGH, had the forecast made
                    h minutes earlier already said HIGH? (persistence cannot)
  5. incidents    - share of closures detected, delay after closure start, and
                    false alarms per hour on the matching runs without closures;
                    reactive detector (current state) vs predictive (5-min forecast)

Usage:
    python3 evaluate.py [--split validation|test] [--levels 1k 5k 10k]
Results are also written to ml/results/<split>.json.
"""

import argparse
import json
import os
import pickle

import numpy as np
import pandas as pd

import dataset
from features import HIGH_BELOW, build_features, profile_speeds, slot_of_day
from segment_minutes import HORIZONS_MIN, SEGMENT_KEY

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(HERE, "models")
RESULTS_DIR = os.path.join(HERE, "results")

MIN_SPEED_FOR_TT = 1.0     # m/s floor so a stopped road gives a finite travel time
STOPPED_ALARM_SHARE = 0.5  # reactive alarm: road HIGH now and most samples stopped


def load_models(models_dir=MODELS_DIR):
    models = {h: pickle.load(open(os.path.join(models_dir, f"speed_forecast_{h}m.pkl"), "rb"))
              for h in HORIZONS_MIN}
    usual = pd.read_csv(os.path.join(models_dir, "usual_speed.csv"))
    with open(os.path.join(models_dir, "model_card.json")) as f:
        features = json.load(f)["features"]
    return models, usual, features


def add_profile_forecast(df, profile):
    """PTDR-style forecast: the segment's usual speed in the slot h minutes ahead.

    Falls back to the segment's usual speed where the profile never saw that
    segment in that slot - PTDR has the same gap, and filling it with the
    segment's own reference is the closest equivalent to its free-flow default.
    """
    for h in HORIZONS_MIN:
        keys = pd.MultiIndex.from_arrays(
            [df["node_from"], df["node_to"], slot_of_day(df["minute"].to_numpy() + h)])
        lookup = profile.set_index(SEGMENT_KEY + ["slot"])["profile_speed"]
        df[f"profile_speed_{h}m"] = lookup.reindex(keys).to_numpy()
        df[f"profile_speed_{h}m"] = df[f"profile_speed_{h}m"].fillna(df["usual_speed"])
    return df


def scored_run(run, models, usual, features, profile=None):
    df = build_features(dataset.state(run), usual)
    for h, model in models.items():
        df[f"pred_ratio_{h}m"] = model.predict(df[features]).clip(0, 2)
    if profile is not None:
        df = add_profile_forecast(df, profile)
    df["affected"] = (dataset.affected_mask(df, dataset.incidents(run))
                      if run.condition == "incidents" else False)
    return df


def prf(truth, pred):
    tp = int((truth & pred).sum())
    precision = tp / max(int(pred.sum()), 1)
    recall = tp / max(int(truth.sum()), 1)
    f1 = 2 * precision * recall / max(precision + recall, 1e-9)
    return round(precision, 3), round(recall, 3), round(f1, 3)


def speed_travel_congestion(df, h):
    rows = df[df[f"ratio_in_{h}m"].notna()]
    out = {}
    for name, mask in (("all roads", np.ones(len(rows), bool)), ("incident-affected", rows["affected"].to_numpy())):
        r = rows[mask]
        if r.empty:
            continue
        actual = r[f"speed_in_{h}m"]
        preds = {"model": r[f"pred_ratio_{h}m"] * r["usual_speed"],
                 "persistence": r["mean_speed"], "usual speed": r["usual_speed"]}
        if f"profile_speed_{h}m" in r:
            preds["profile"] = r[f"profile_speed_{h}m"]
        tt = lambda s: r["segment_length"] / np.maximum(s, MIN_SPEED_FOR_TT)
        out[name] = {
            "rows": len(r),
            "speed_mae": {k: round(float(np.abs(actual - p).mean()), 3) for k, p in preds.items()},
            "travel_time_mae_s": {k: round(float(np.abs(tt(actual) - tt(p)).mean()), 2) for k, p in preds.items()},
        }
    truth = rows[f"ratio_in_{h}m"] < HIGH_BELOW
    out["congestion_HIGH"] = {
        "actual_high_rows": int(truth.sum()),
        "model_p_r_f1": prf(truth, rows[f"pred_ratio_{h}m"] < HIGH_BELOW),
        "persistence_p_r_f1": prf(truth, rows["ratio_now"] < HIGH_BELOW),
    }
    if f"profile_speed_{h}m" in rows:
        out["congestion_HIGH"]["profile_p_r_f1"] = prf(
            truth, rows[f"profile_speed_{h}m"] / rows["usual_speed"] < HIGH_BELOW)
    return out


def early_warning(df, h):
    """Onsets: affected road-minutes that are HIGH now but were not HIGH the previous observed minute."""
    d = df.sort_values(["node_from", "node_to", "minute"])
    high = d["ratio_now"] < HIGH_BELOW
    same_seg = (d["node_from"].eq(d["node_from"].shift())) & (d["node_to"].eq(d["node_to"].shift()))
    prev_high = high.shift(fill_value=False) & same_seg
    onsets = d[high & ~prev_high & d["affected"]]
    lookup = d.set_index(["node_from", "node_to", "minute"])
    keys = pd.MultiIndex.from_arrays([onsets["node_from"], onsets["node_to"], onsets["minute"] - h])
    earlier = lookup.reindex(keys)
    has_earlier = earlier["ratio_now"].notna().to_numpy()
    warned = (earlier[f"pred_ratio_{h}m"] < HIGH_BELOW).to_numpy() & has_earlier
    persisted = (earlier["ratio_now"] < HIGH_BELOW).to_numpy() & has_earlier
    n = int(has_earlier.sum())
    return {"onsets": len(onsets), "onsets_with_road_seen_h_min_before": n,
            "warned_by_model_pct": round(100 * warned.sum() / max(n, 1), 1),
            "warned_by_persistence_pct": round(100 * persisted.sum() / max(n, 1), 1)}


def alarms(df):
    reactive = (df["ratio_now"] < HIGH_BELOW) & (df["stopped_share"] >= STOPPED_ALARM_SHARE)
    predictive = reactive | (df[f"pred_ratio_{HORIZONS_MIN[0]}m"] < HIGH_BELOW)
    return {"reactive": reactive.to_numpy(), "predictive": predictive.to_numpy()}


def incident_detection(incident_df, incident_run, control_df):
    inc = dataset.incidents(incident_run)
    result = {}
    fired = alarms(incident_df)
    control_fired = alarms(control_df)
    hours = (control_df["minute"].max() - control_df["minute"].min()) / 60
    for name in ("reactive", "predictive"):
        delays = []
        for c in inc.itertuples():
            near = ((incident_df["node_from"] == c.node_from) & (incident_df["node_to"] == c.node_to)) | \
                   (incident_df["node_to"] == c.node_from)
            during = (incident_df["minute"] >= c.start_minute) & (incident_df["minute"] <= c.end_minute)
            hits = incident_df.loc[near.to_numpy() & during.to_numpy() & fired[name], "minute"]
            if len(hits):
                delays.append(int(hits.min() - c.start_minute))
        result[name] = {"closures": len(inc), "detected": len(delays),
                        "median_delay_min": float(np.median(delays)) if delays else None,
                        "false_alarm_road_minutes_per_hour": round(float(control_fired[name].sum() / hours), 1)}
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="validation", choices=["validation", "test"])
    ap.add_argument("--levels", nargs="+", default=list(dataset.LEVELS))
    ap.add_argument("--models-dir", default=MODELS_DIR)
    ap.add_argument("--name", help="results file name (default: the split)")
    args = ap.parse_args()

    models, usual, features = load_models(args.models_dir)
    results = {}
    for level in args.levels:
        # PTDR-style profile: built from the same training runs the model saw,
        # per level, so both forecasters learn from exactly the same history.
        train_runs = dataset.runs("train", levels=[level], conditions=("control",))
        profile = profile_speeds([dataset.state(r) for r in train_runs]) if train_runs else None
        if profile is not None:
            print(f"{level}: profile from {', '.join(str(r) for r in train_runs)}"
                  f" ({len(profile):,} segment-slots)")
        runs = dataset.runs(args.split, levels=[level])
        scored = {r.condition: (r, scored_run(r, models, usual, features, profile)) for r in runs}
        both = pd.concat([df for _, df in scored.values()], ignore_index=True)
        res = {"runs": [str(r) for r in runs], "horizons": {}}
        for h in HORIZONS_MIN:
            res["horizons"][f"{h}m"] = speed_travel_congestion(both, h)
            if "incidents" in scored:
                res["horizons"][f"{h}m"]["early_warning"] = early_warning(scored["incidents"][1], h)
        if "incidents" in scored and "control" in scored:
            res["incident_detection"] = incident_detection(scored["incidents"][1], scored["incidents"][0],
                                                           scored["control"][1])
        results[level] = res
        print_level(level, res)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out = os.path.join(RESULTS_DIR, f"{args.name or args.split}.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nwritten {out}")


def print_level(level, res):
    print(f"\n=== {level} ({', '.join(res['runs'])}) ===")
    for h, r in res["horizons"].items():
        print(f"  horizon {h}")
        for subset in ("all roads", "incident-affected"):
            if subset in r:
                s = r[subset]
                print(f"    {subset:<18} rows {s['rows']:>8,} | speed MAE m/s  "
                      + "  ".join(f"{k} {v:.2f}" for k, v in s["speed_mae"].items())
                      + " | travel time MAE s  "
                      + "  ".join(f"{k} {v:.1f}" for k, v in s["travel_time_mae_s"].items()))
        c = r["congestion_HIGH"]
        print(f"    congestion HIGH    actual rows {c['actual_high_rows']:,} | P/R/F1 model {c['model_p_r_f1']}"
              f"  persistence {c['persistence_p_r_f1']}"
              + (f"  profile {c['profile_p_r_f1']}" if "profile_p_r_f1" in c else ""))
        if "early_warning" in r:
            w = r["early_warning"]
            print(f"    early warning      {w['onsets_with_road_seen_h_min_before']} jam onsets with history |"
                  f" warned: model {w['warned_by_model_pct']}%  persistence {w['warned_by_persistence_pct']}%")
    if "incident_detection" in res:
        for name, d in res["incident_detection"].items():
            print(f"  incidents {name:<10} detected {d['detected']}/{d['closures']}, median delay"
                  f" {d['median_delay_min']} min, false alarms {d['false_alarm_road_minutes_per_hour']} road-min/h")


if __name__ == "__main__":
    main()
