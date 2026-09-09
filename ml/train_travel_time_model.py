"""Train the travel-time regressor on windowed traffic features."""

import os
import pickle
import sys

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import WINDOW_SECONDS, build_windowed_dataset, load_fcd_df  # noqa: E402

MODEL_PARAMS = dict(
    n_estimators=100,
    max_depth=20,
    min_samples_split=5,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=1,
)

TRAVEL_TIME_FEATURES = [
    "segment_length",
    "avg_speed",
    "max_speed",
    "vehicle_count",
    "observation_count",
]


def train_travel_time_model(h5_path: str, output_path: str = "models/travel_time_model.pkl"):
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print(f"Building {WINDOW_SECONDS}s-window features...")
    data = build_windowed_dataset(df)

    X = data[TRAVEL_TIME_FEATURES]
    y = data["travel_time"]

    print(f"Training data shape: {X.shape}")
    print("Travel time statistics:")
    print(f"  Mean: {y.mean():.2f}s")
    print(f"  Min:  {y.min():.2f}s")
    print(f"  Max:  {y.max():.2f}s")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print("Training RandomForest regressor...")
    model = RandomForestRegressor(**MODEL_PARAMS)
    model.fit(X_train, y_train)

    y_test_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_test_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))

    print(f"Train R²:  {model.score(X_train, y_train):.4f}")
    print(f"Test R²:   {r2_score(y_test, y_test_pred):.4f}")
    print(f"Test MAE:  {mae:.2f}s")
    print(f"Test RMSE: {rmse:.2f}s")

    print(f"Saving model to {output_path}...")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(model, f)

    print("✅ Travel time model trained successfully!")
    return model


if __name__ == "__main__":
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD100.h5"
    train_travel_time_model(h5_file)
