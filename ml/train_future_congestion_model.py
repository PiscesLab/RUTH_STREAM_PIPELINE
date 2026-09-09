"""Train the future-congestion classifier on windowed traffic features.

The target here is what actually happened next: the congestion class observed
on the same segment over the following HORIZON_SECONDS. The previous version
derived the "future" label from a rule applied to the present values, so the
model could only learn to reproduce that rule. Training against the measured
outcome makes this a real forecasting task - expect a lower, but meaningful,
accuracy.
"""

import os
import pickle
import sys

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import (  # noqa: E402
    HORIZON_SECONDS,
    WINDOW_SECONDS,
    build_windowed_dataset,
    load_fcd_df,
)

MODEL_PARAMS = dict(
    n_estimators=100,
    max_depth=20,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=1,
)

FUTURE_FEATURES = [
    "avg_speed",
    "max_speed",
    "min_speed",
    "std_speed",
    "vehicle_count",
    "observation_count",
    "vehicle_type_diversity",
    "current_congestion",
]


def train_future_congestion_model(
    h5_path: str, output_path: str = "models/future_congestion_model.pkl"
):
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print(f"Building {WINDOW_SECONDS}s-window features "
          f"(predicting {HORIZON_SECONDS}s ahead)...")
    data = build_windowed_dataset(df)

    X = data[FUTURE_FEATURES]
    y = data["future_congestion"]

    print(f"Training data shape: {X.shape}")
    print(f"Future congestion distribution:\n{y.value_counts()}")

    # Split on time so the model is validated on later traffic than it saw in
    # training. A random split would let neighbouring, overlapping windows land
    # on both sides and inflate the score.
    data = data.sort_values("timestamp")
    split_at = int(len(data) * 0.8)
    train, test = data.iloc[:split_at], data.iloc[split_at:]
    X_train, y_train = train[FUTURE_FEATURES], train["future_congestion"]
    X_test, y_test = test[FUTURE_FEATURES], test["future_congestion"]
    print(f"Chronological split: {len(train)} train / {len(test)} test")

    print("Training RandomForest classifier...")
    model = RandomForestClassifier(**MODEL_PARAMS)
    model.fit(X_train, y_train)

    y_test_pred = model.predict(X_test)

    print(f"Train Accuracy: {model.score(X_train, y_train):.4f}")
    print(f"Test Accuracy:  {model.score(X_test, y_test):.4f}")
    print(f"Test F1 Score:  {f1_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print(f"Test Precision: {precision_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print(f"Test Recall:    {recall_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print(f"Macro F1:       {f1_score(y_test, y_test_pred, average='macro', zero_division=0):.4f}")
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_test_pred))
    print("\nClassification Report:")
    print(classification_report(y_test, y_test_pred, zero_division=0))

    print(f"Saving model to {output_path}...")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(model, f)

    print("✅ Future congestion model trained successfully!")
    return model


if __name__ == "__main__":
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD100.h5"
    train_future_congestion_model(h5_file)
