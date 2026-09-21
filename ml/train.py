#!/usr/bin/env python3
"""Train the road-speed forecast models (one per horizon) on the training seeds.

Each model predicts a segment's speed HORIZON minutes ahead as a ratio of its
usual speed. Congestion level, travel time and incident warnings are all
derived from that forecast, so the pipeline runs one model per horizon.

Outputs in ml/models/:
    speed_forecast_5m.pkl, speed_forecast_15m.pkl
    usual_speed.csv         reference speed per segment (from training controls)
    model_card.json         features, horizons, thresholds, training runs, sizes

Usage:
    python3 train.py [--levels 1k 5k 10k]
"""

import argparse
import json
import os
import pickle
import time
from datetime import datetime

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingRegressor

import dataset
from features import FEATURE_SETS, HIGH_BELOW, MEDIUM_BELOW, build_features, usual_speeds
from segment_minutes import BIN_SECONDS, HORIZONS_MIN

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")

# Chosen on the validation seed (seed 3). Absolute-error loss beat squared error
# on every speed measure: per-vehicle speed noise is heavy-tailed, and the
# forecast is scored by absolute error. Up-weighting incident-affected rows
# (x5) changed little, so rows are weighted equally.
AFFECTED_WEIGHT = 1.0

MODEL_PARAMS = dict(loss="absolute_error", max_iter=300, learning_rate=0.1, max_leaf_nodes=31,
                    min_samples_leaf=50, l2_regularization=1.0, random_state=0)


def training_table(levels):
    train_runs = dataset.runs("train", levels=levels)
    usual = usual_speeds([dataset.state(r) for r in train_runs if r.condition == "control"])
    parts = []
    for run in train_runs:
        df = build_features(dataset.state(run), usual)
        df["affected"] = (dataset.affected_mask(df, dataset.incidents(run))
                          if run.condition == "incidents" else False)
        df["run"] = str(run)
        parts.append(df)
        print(f"  {run}: {len(df):,} segment-minutes")
    return pd.concat(parts, ignore_index=True), usual, train_runs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--levels", nargs="+", default=list(dataset.LEVELS))
    ap.add_argument("--loss", default=MODEL_PARAMS["loss"], choices=["squared_error", "absolute_error"])
    ap.add_argument("--affected-weight", type=float, default=AFFECTED_WEIGHT)
    ap.add_argument("--models-dir", default=MODELS_DIR, help="write somewhere else for tuning runs")
    # "local" chosen on the validation seed: the downstream features changed speed
    # error by <= 0.1 m/s, and without them a twin needs no messages from other twins
    ap.add_argument("--features", default="local", choices=sorted(FEATURE_SETS))
    args = ap.parse_args()
    features = FEATURE_SETS[args.features]
    params = dict(MODEL_PARAMS, loss=args.loss)
    models_dir = args.models_dir

    print("Building training data (seeds 1-2)...")
    data, usual, train_runs = training_table(args.levels)
    os.makedirs(models_dir, exist_ok=True)
    usual.to_csv(os.path.join(models_dir, "usual_speed.csv"), index=False)

    card = {
        "created": datetime.now().isoformat(timespec="seconds"),
        "sklearn": sklearn.__version__,
        "target": "segment mean speed h minutes ahead / usual_speed",
        "bin_seconds": BIN_SECONDS,
        "feature_set": args.features,
        "features": features,
        "congestion_thresholds": {"HIGH_below_ratio": HIGH_BELOW, "MEDIUM_below_ratio": MEDIUM_BELOW},
        "affected_weight": args.affected_weight,
        "params": params,
        "train_runs": [str(r) for r in train_runs],
        "usual_speed_segments": len(usual),
        "models": {},
    }

    for h in HORIZONS_MIN:
        target = f"ratio_in_{h}m"
        rows = data[data[target].notna()]
        X, y = rows[features], rows[target].clip(0, 2)
        w = np.where(rows["affected"], args.affected_weight, 1.0)

        print(f"\nHorizon {h} min: {len(rows):,} rows ({rows['affected'].sum():,} incident-affected)")
        t0 = time.time()
        model = HistGradientBoostingRegressor(**params).fit(X, y, sample_weight=w)
        fit_s = time.time() - t0

        t0 = time.time()
        model.predict(X.iloc[:10_000])
        per_row_ms = (time.time() - t0) / 10_000 * 1000

        path = os.path.join(models_dir, f"speed_forecast_{h}m.pkl")
        with open(path, "wb") as f:
            pickle.dump(model, f)
        size_mb = os.path.getsize(path) / 1e6
        print(f"  trained in {fit_s:.0f} s, {model.n_iter_} trees, {size_mb:.1f} MB,"
              f" batch inference {per_row_ms * 1000:.1f} us/row")
        card["models"][f"{h}m"] = {"file": os.path.basename(path), "rows": len(rows),
                                    "affected_rows": int(rows["affected"].sum()),
                                    "trees": int(model.n_iter_), "size_mb": round(size_mb, 2),
                                    "fit_seconds": round(fit_s, 1),
                                    "batch_inference_us_per_row": round(per_row_ms * 1000, 2)}

    with open(os.path.join(models_dir, "model_card.json"), "w") as f:
        json.dump(card, f, indent=2)
    print(f"\nSaved models, usual_speed.csv and model_card.json to {models_dir}")


if __name__ == "__main__":
    main()
