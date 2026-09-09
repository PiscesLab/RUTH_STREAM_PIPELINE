"""Train the travel-time regressor.

Target: how long a vehicle *actually* takes to cross a segment, measured from
its own FCD samples. Features use only what is known when it enters, so the
model has to forecast rather than restate.

The previous target was `segment_length / avg_speed`, computed from two of the
model's own inputs - so it scored R2 0.9997 while learning nothing a division
does not do exactly. That number looked excellent and meant nothing.
"""

import os
import pickle
import sys

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import (  # noqa: E402
    CROSSING_FEATURE_COLUMNS,
    WINDOW_SECONDS,
    build_crossing_dataset,
    load_fcd_df,
)

# max_leaf_nodes caps how large each tree can grow, so model size stays bounded
# no matter how much data is added. Unbounded trees reached 455MB on 264k rows
# and scored *worse* on held-out data (macro F1 0.666/0.728 against 0.699/0.751
# here) - they were memorising rather than generalising.
MODEL_PARAMS = dict(
    n_estimators=100,
    max_leaf_nodes=20000,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    # fit across all cores; the saved model is switched to n_jobs=1 below so
    # that single-row inference in the pipeline does not fan out per prediction
    n_jobs=-1,
)


def _evaluate(model, data, label):
    """Score the model on a dataset it was not trained on."""
    pred = model.predict(data[CROSSING_FEATURE_COLUMNS])
    print(f"\n  {label}  (n={len(data):,})")
    print(f"    length/entry_speed {mean_absolute_error(data['crossing_time'], data['naive_estimate']):6.2f}s"
          f"  -> model {mean_absolute_error(data['crossing_time'], pred):6.2f}s MAE")


def train_travel_time_model(h5_path: str, output_path: str = "models/travel_time_model.pkl",
                            holdout_paths=None):
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print(f"Building crossing dataset ({WINDOW_SECONDS}s window for context)...")
    data = build_crossing_dataset(df)

    X = data[CROSSING_FEATURE_COLUMNS]
    y = data["crossing_time"]

    print(f"Training data shape: {X.shape}")
    print("Actual crossing time:")
    print(f"  Mean:   {y.mean():.2f}s")
    print(f"  Median: {y.median():.2f}s")
    print(f"  Max:    {y.max():.2f}s")

    # chronological split: validate on later traffic than was trained on
    split_at = int(len(data) * 0.8)
    train, test = data.iloc[:split_at], data.iloc[split_at:]
    X_train, y_train = train[CROSSING_FEATURE_COLUMNS], train["crossing_time"]
    X_test, y_test = test[CROSSING_FEATURE_COLUMNS], test["crossing_time"]
    print(f"Chronological split: {len(train)} train / {len(test)} test")

    print("Training RandomForest regressor...")
    model = RandomForestRegressor(**MODEL_PARAMS)
    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    rmse = np.sqrt(mean_squared_error(y_test, pred))

    # the estimate the pipeline would produce without a model at all
    naive_mae = mean_absolute_error(y_test, test["naive_estimate"])
    mean_mae = mean_absolute_error(y_test, [y_train.mean()] * len(y_test))

    print(f"\nTest R²:   {r2_score(y_test, pred):.4f}")
    print(f"Test MAE:  {mae:.2f}s")
    print(f"Test RMSE: {rmse:.2f}s")
    print("\nCompared with predicting without a model:")
    print(f"  always predict the mean       : {mean_mae:6.2f}s MAE")
    print(f"  segment_length / entry_speed  : {naive_mae:6.2f}s MAE")
    print(f"  this model                    : {mae:6.2f}s MAE"
          f"   ({(naive_mae - mae) / naive_mae * 100:+.0f}% vs the formula)")
    print("\nFeature importances:")
    for name, imp in sorted(
        zip(CROSSING_FEATURE_COLUMNS, model.feature_importances_),
        key=lambda kv: -kv[1],
    ):
        print(f"  {name:<20}{imp:.3f}")

    for path in holdout_paths or []:
        try:
            held = build_crossing_dataset(load_fcd_df(path))
            _evaluate(model, held, f"HELD OUT: {os.path.basename(path)}")
        except Exception as exc:
            print(f"\n  HELD OUT: {os.path.basename(path)} - skipped ({exc})")

    # single-row predictions in the pipeline are faster on one core
    model.set_params(n_jobs=1)

    print(f"\nSaving model to {output_path}...")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(model, f)

    print("✅ Travel time model trained successfully!")
    return model


if __name__ == "__main__":
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD1k.h5"
    train_travel_time_model(h5_file, holdout_paths=sys.argv[2:])
