"""Train the future-traffic model.

Target: the mean speed actually observed on the segment over the next
HORIZON_SECONDS. The pipeline still reports HIGH/MEDIUM/LOW, but that class is
derived by thresholding the predicted speed rather than predicted directly.

Predicting the class directly does not work on this data. 74% of windows are
MEDIUM, so a classifier learns to say MEDIUM: it scored 74.7% accuracy against
a 77.5% always-guess-MEDIUM baseline - worse than guessing - with macro F1
0.48. Regressing the speed and thresholding afterwards uses the full signal
instead of collapsing it into three buckets, and reaches 82.7% / macro F1 0.59,
beating both the majority and persistence baselines.
"""

import os
import pickle
import sys

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import (  # noqa: E402
    HORIZON_SECONDS,
    WINDOW_SECONDS,
    build_windowed_dataset,
    congestion_label,
    load_fcd_df,
)

MODEL_PARAMS = dict(
    n_estimators=100,
    max_depth=20,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    # fit across all cores; the saved model is switched to n_jobs=1 below so
    # that single-row inference in the pipeline does not fan out per prediction
    n_jobs=-1,
)

FUTURE_FEATURES = [
    "avg_speed",
    "max_speed",
    "min_speed",
    "std_speed",
    "vehicle_count",
    "observation_count",
    "vehicle_type_diversity",
    "segment_length",
]


def _evaluate(model, data, label):
    """Score the model on a dataset it was not trained on."""
    pred = model.predict(data[FUTURE_FEATURES])
    persistence = mean_absolute_error(data["future_speed"], data["avg_speed"])
    model_mae = mean_absolute_error(data["future_speed"], pred)
    truth_cls = data["future_congestion"]
    pred_cls = np.array([congestion_label(v) for v in pred])
    majority = truth_cls.value_counts().idxmax()
    print(f"\n  {label}  (n={len(data):,})")
    print(f"    speed MAE   persistence {persistence:6.3f} -> model {model_mae:6.3f} m/s"
          f"  ({(persistence - model_mae) / persistence * 100:+.0f}%)")
    print(f"    class acc   majority {(truth_cls == majority).mean() * 100:5.1f}%"
          f"  -> model {(pred_cls == truth_cls).mean() * 100:5.1f}%"
          f"   macro F1 {f1_score(truth_cls, pred_cls, average='macro', zero_division=0):.3f}")


def train_future_congestion_model(
    h5_path: str, output_path: str = "models/future_congestion_model.pkl",
    holdout_paths=None,
):
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print(f"Building {WINDOW_SECONDS}s-window features "
          f"(predicting {HORIZON_SECONDS}s ahead)...")
    data = build_windowed_dataset(df).sort_values("timestamp")

    print(f"Training data shape: ({len(data)}, {len(FUTURE_FEATURES)})")
    print(f"Future congestion distribution:\n{data['future_congestion'].value_counts()}")

    split_at = int(len(data) * 0.8)
    train, test = data.iloc[:split_at], data.iloc[split_at:]
    X_train, y_train = train[FUTURE_FEATURES], train["future_speed"]
    X_test, y_test = test[FUTURE_FEATURES], test["future_speed"]
    print(f"Chronological split: {len(train)} train / {len(test)} test")

    print("Training RandomForest regressor (speed, m/s)...")
    model = RandomForestRegressor(**MODEL_PARAMS)
    model.fit(X_train, y_train)

    pred_speed = model.predict(X_test)

    # regression quality, against predicting that nothing changes
    persistence_mae = mean_absolute_error(y_test, test["avg_speed"])
    model_mae = mean_absolute_error(y_test, pred_speed)
    print(f"\nFuture speed MAE:")
    print(f"  persistence (future = now)    : {persistence_mae:6.3f} m/s")
    print(f"  this model                    : {model_mae:6.3f} m/s"
          f"   ({(persistence_mae - model_mae) / persistence_mae * 100:+.0f}%)")

    # the class the pipeline actually reports
    truth_cls = test["future_congestion"]
    pred_cls = np.array([congestion_label(v) for v in pred_speed])
    majority = truth_cls.value_counts().idxmax()
    persist_cls = test["avg_speed"].apply(congestion_label)

    acc = (pred_cls == truth_cls).mean()
    print("\nDerived congestion class:")
    print(f"  always guess '{majority}'        : {(truth_cls == majority).mean() * 100:5.1f}%"
          f"   macro F1 {f1_score(truth_cls, [majority] * len(truth_cls), average='macro', zero_division=0):.3f}")
    print(f"  persistence (same as now)     : {(persist_cls == truth_cls).mean() * 100:5.1f}%"
          f"   macro F1 {f1_score(truth_cls, persist_cls, average='macro', zero_division=0):.3f}")
    print(f"  this model                    : {acc * 100:5.1f}%"
          f"   macro F1 {f1_score(truth_cls, pred_cls, average='macro', zero_division=0):.3f}")

    print("\nConfusion Matrix:")
    print(confusion_matrix(truth_cls, pred_cls))
    print("\nClassification Report:")
    print(classification_report(truth_cls, pred_cls, zero_division=0))

    # the real test: data the model has never seen, ideally another road network
    for path in holdout_paths or []:
        try:
            held = build_windowed_dataset(load_fcd_df(path))
            _evaluate(model, held, f"HELD OUT: {os.path.basename(path)}")
        except Exception as exc:
            print(f"\n  HELD OUT: {os.path.basename(path)} - skipped ({exc})")

    # single-row predictions in the pipeline are faster on one core
    model.set_params(n_jobs=1)

    print(f"Saving model to {output_path}...")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(model, f)

    print("✅ Future traffic model trained successfully!")
    return model


if __name__ == "__main__":
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD1k.h5"
    train_future_congestion_model(h5_file, holdout_paths=sys.argv[2:])
