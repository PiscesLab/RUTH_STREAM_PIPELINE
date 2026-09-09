"""Train the current-congestion classifier on windowed traffic features."""

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
    FEATURE_COLUMNS,
    WINDOW_SECONDS,
    build_windowed_dataset,
    load_fcd_df,
)

# The live pipeline predicts one segment at a time, so a single-threaded
# forest avoids the thread fan-out cost of n_jobs=-1 on a one-row predict.
MODEL_PARAMS = dict(
    n_estimators=100,
    max_depth=20,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=1,
)

CONGESTION_FEATURES = [
    "avg_speed",
    "max_speed",
    "min_speed",
    "std_speed",
    "vehicle_count",
    "observation_count",
]


def train_congestion_model(h5_path: str, output_path: str = "models/congestion_model.pkl"):
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print(f"Building {WINDOW_SECONDS}s-window features...")
    data = build_windowed_dataset(df)

    X = data[CONGESTION_FEATURES]
    y = data["congestion"]

    print(f"Training data shape: {X.shape}")
    print(f"Congestion distribution:\n{y.value_counts()}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("Training RandomForest classifier...")
    model = RandomForestClassifier(**MODEL_PARAMS)
    model.fit(X_train, y_train)

    y_test_pred = model.predict(X_test)

    print(f"Train Accuracy: {model.score(X_train, y_train):.4f}")
    print(f"Test Accuracy:  {model.score(X_test, y_test):.4f}")
    print(f"Test F1 Score:  {f1_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print(f"Test Precision: {precision_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print(f"Test Recall:    {recall_score(y_test, y_test_pred, average='weighted', zero_division=0):.4f}")
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_test_pred))
    print("\nClassification Report:")
    print(classification_report(y_test, y_test_pred, zero_division=0))

    print(f"Saving model to {output_path}...")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(model, f)

    print("✅ Congestion model trained successfully!")
    return model


if __name__ == "__main__":
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD100.h5"
    train_congestion_model(h5_file)
