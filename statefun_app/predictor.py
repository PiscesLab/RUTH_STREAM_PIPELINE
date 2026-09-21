"""Road-twin predictions for the streaming pipeline.

Loads, once per process, what ml/train.py produced in ml/models/:
    speed_forecast_5m.pkl, speed_forecast_15m.pkl, usual_speed.csv, model_card.json

The feature list is read from model_card.json and the features are computed by
ml/features.py (twin_features), the same code the models were trained with, so
serving cannot drift from training.
"""

import csv
import json
import os
import pickle
import sys

import numpy as np

ML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ml")
MODELS_DIR = os.path.join(ML_DIR, "models")
sys.path.insert(0, ML_DIR)

from features import (  # noqa: E402
    HIGH_BELOW, MEDIUM_BELOW, STOPPED_ALARM_SHARE, twin_features)

MIN_SPEED_FOR_TRAVEL_TIME = 1.0  # m/s, keeps a stopped road's travel time finite


class Forest:
    """The trees of a trained HistGradientBoostingRegressor, evaluated for one road.

    A twin predicts for its own road only, so every call is a single row.
    scikit-learn's predict() costs ~7 ms whatever the number of rows - it is
    per-call checking, not tree work (1000 rows cost the same as 1). Walking
    the trees directly costs tens of microseconds and returns the same value;
    the parity is asserted in the tests below this module's users.
    """

    def __init__(self, model):
        self.baseline = float(model._baseline_prediction)
        nodes, offsets = [], []
        for predictors in model._predictors:          # one per boosting iteration
            tree = predictors[0].nodes
            offsets.append(sum(len(n) for n in nodes))
            nodes.append(tree)
        allnodes = np.concatenate(nodes)
        self.offset = np.array(offsets, dtype=np.int64)
        self.is_leaf = allnodes["is_leaf"].astype(bool)
        self.value = allnodes["value"].astype(np.float64)
        self.feature = allnodes["feature_idx"].astype(np.int64)
        self.threshold = allnodes["num_threshold"].astype(np.float64)
        self.missing_left = allnodes["missing_go_to_left"].astype(bool)
        self.left = allnodes["left"].astype(np.int64)
        self.right = allnodes["right"].astype(np.int64)
        # left/right are indices within each tree; make them absolute once
        tree_of_node = np.repeat(self.offset, [len(n) for n in nodes])
        self.left += tree_of_node
        self.right += tree_of_node

    def predict_one(self, x):
        node = self.offset.copy()
        while True:
            walking = ~self.is_leaf[node]
            if not walking.any():
                break
            here = node[walking]
            values = x[self.feature[here]]
            go_left = np.where(np.isnan(values), self.missing_left[here],
                               values <= self.threshold[here])
            node[walking] = np.where(go_left, self.left[here], self.right[here])
        return self.baseline + float(self.value[node].sum())


def congestion(ratio):
    if ratio < HIGH_BELOW:
        return "HIGH"
    if ratio < MEDIUM_BELOW:
        return "MEDIUM"
    return "LOW"


def travel_time_s(segment_length, speed):
    return segment_length / max(speed, MIN_SPEED_FOR_TRAVEL_TIME)


class Predictor:
    def __init__(self, models_dir=MODELS_DIR):
        with open(os.path.join(models_dir, "model_card.json")) as f:
            card = json.load(f)
        self.features = card["features"]
        self.models = {}
        for name, info in card["models"].items():          # "5m", "15m"
            with open(os.path.join(models_dir, info["file"]), "rb") as f:
                self.models[name] = Forest(pickle.load(f))
        self.usual = {}
        with open(os.path.join(models_dir, "usual_speed.csv")) as f:
            for row in csv.DictReader(f):
                self.usual[f"{row['node_from']}_{row['node_to']}"] = float(row["usual_speed"])
        self.fallback_speed = float(np.median(list(self.usual.values())))

    def usual_speed(self, segment_id):
        return max(self.usual.get(segment_id, self.fallback_speed), 1.0)

    def current(self, segment_id, segment_length, mean_speed, stopped_share):
        """What the twin knows without a model: congestion, travel time, incident alert."""
        ratio = mean_speed / self.usual_speed(segment_id)
        return {
            "usual_speed_mps": round(self.usual_speed(segment_id), 2),
            "congestion": congestion(ratio),
            "travel_time_s": round(travel_time_s(segment_length, mean_speed), 1),
            "incident_alert": bool(ratio < HIGH_BELOW and stopped_share >= STOPPED_ALARM_SHARE),
        }

    def forecast(self, segment_id, segment_length, history):
        """Speed, congestion and travel time 5 and 15 minutes ahead from the twin's history."""
        usual = self.usual_speed(segment_id)
        x = np.array(twin_features(history, usual, segment_length, self.features), dtype=float)
        out = {}
        for name, forest in self.models.items():
            ratio = float(np.clip(forest.predict_one(x), 0.0, 2.0))
            speed = ratio * usual
            out[name] = {"speed_mps": round(speed, 2), "congestion": congestion(ratio),
                         "travel_time_s": round(travel_time_s(segment_length, speed), 1)}
        return out
