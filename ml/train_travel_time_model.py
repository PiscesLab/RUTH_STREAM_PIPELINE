import pickle
import numpy as np
import pandas as pd
import h5py
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import json

def load_fcd_df(h5_path: str, dataset_key: str = "fcd") -> pd.DataFrame:
    """Load FCD data from H5 file"""
    with h5py.File(h5_path, "r") as f:
        if dataset_key not in f:
            raise KeyError(f"Dataset key '{dataset_key}' not found")
        data = f[dataset_key][:]

    df = pd.DataFrame.from_records(data)

    if "vehicle_type" in df.columns:
        df["vehicle_type"] = df["vehicle_type"].apply(
            lambda x: x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)
        )

    return df

def engineer_features(df: pd.DataFrame) -> tuple:
    """Engineer features for travel time prediction"""
    # Create segment_id from node_from and node_to
    df['segment_id'] = df['node_from'].astype(str) + '_' + df['node_to'].astype(str)

    segment_features = []

    for segment_id in df['segment_id'].unique():
        segment_data = df[df['segment_id'] == segment_id]

        avg_speed = segment_data['speed_mps'].mean()
        segment_length = segment_data['segment_length'].iloc[0]
        vehicle_count = len(segment_data)
        max_speed = segment_data['speed_mps'].max()

        # Calculate actual travel time
        if avg_speed > 0:
            travel_time = segment_length / avg_speed
        else:
            travel_time = segment_length / 0.1

        segment_features.append({
            'segment_id': segment_id,
            'segment_length': segment_length,
            'avg_speed': avg_speed,
            'max_speed': max_speed,
            'vehicle_count': vehicle_count,
            'travel_time': travel_time
        })

    features_df = pd.DataFrame(segment_features)
    features_df = features_df.fillna(0)

    X = features_df[['segment_length', 'avg_speed', 'max_speed', 'vehicle_count']]
    y = features_df['travel_time']

    return X, y

def train_travel_time_model(h5_path: str, output_path: str = "models/travel_time_model.pkl"):
    """Train RandomForest regressor for travel time prediction"""
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print("Engineering features...")
    X, y = engineer_features(df)

    print(f"Training data shape: {X.shape}")
    print(f"Travel time statistics:")
    print(f"  Mean: {y.mean():.2f}s")
    print(f"  Min: {y.min():.2f}s")
    print(f"  Max: {y.max():.2f}s")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print("Training RandomForest regressor...")
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=20,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    test_mae = mean_absolute_error(y_test, y_test_pred)

    print(f"Train R² Score: {train_score:.4f}")
    print(f"Test R² Score:  {test_score:.4f}")
    print(f"Test MAE:       {test_mae:.4f}s")
    print(f"Train RMSE:     {train_rmse:.4f}s")
    print(f"Test RMSE:      {test_rmse:.4f}s")

    print(f"Saving model to {output_path}...")
    with open(output_path, 'wb') as f:
        pickle.dump(model, f)

    print("✅ Travel time model trained successfully!")
    return model

if __name__ == "__main__":
    import sys
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "inputfiles/SanDiegoFCD1k.h5"
    train_travel_time_model(h5_file)
