import pickle
import numpy as np
import pandas as pd
import h5py
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, precision_score, recall_score, confusion_matrix, classification_report
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
    """Engineer features for congestion prediction"""
    # Create segment_id from node_from and node_to
    df['segment_id'] = df['node_from'].astype(str) + '_' + df['node_to'].astype(str)

    # Group by segment and calculate aggregate features
    segment_features = []

    for segment_id in df['segment_id'].unique():
        segment_data = df[df['segment_id'] == segment_id]

        avg_speed = segment_data['speed_mps'].mean()
        max_speed = segment_data['speed_mps'].max()
        min_speed = segment_data['speed_mps'].min()
        std_speed = segment_data['speed_mps'].std()
        vehicle_count = len(segment_data)

        # Determine congestion label (ground truth from average speed)
        if avg_speed < 5:
            congestion = "HIGH"
        elif avg_speed < 12:
            congestion = "MEDIUM"
        else:
            congestion = "LOW"

        segment_features.append({
            'segment_id': segment_id,
            'avg_speed': avg_speed,
            'max_speed': max_speed,
            'min_speed': min_speed,
            'std_speed': std_speed,
            'vehicle_count': vehicle_count,
            'congestion': congestion
        })

    features_df = pd.DataFrame(segment_features)
    features_df = features_df.fillna(0)

    X = features_df[['avg_speed', 'max_speed', 'min_speed', 'std_speed', 'vehicle_count']]
    y = features_df['congestion']

    return X, y

def train_congestion_model(h5_path: str, output_path: str = "models/congestion_model.pkl"):
    """Train RandomForest classifier for congestion prediction"""
    print("Loading FCD data...")
    df = load_fcd_df(h5_path)

    print("Engineering features...")
    X, y = engineer_features(df)

    print(f"Training data shape: {X.shape}")
    print(f"Congestion distribution:\n{y.value_counts()}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("Training RandomForest classifier...")
    model = RandomForestClassifier(
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
    test_f1 = f1_score(y_test, y_test_pred, average='weighted', zero_division=0)
    test_precision = precision_score(y_test, y_test_pred, average='weighted', zero_division=0)
    test_recall = recall_score(y_test, y_test_pred, average='weighted', zero_division=0)

    print(f"Train Accuracy: {train_score:.4f}")
    print(f"Test Accuracy:  {test_score:.4f}")
    print(f"Test F1 Score:  {test_f1:.4f}")
    print(f"Test Precision: {test_precision:.4f}")
    print(f"Test Recall:    {test_recall:.4f}")
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_test_pred))
    print("\nClassification Report:")
    print(classification_report(y_test, y_test_pred))

    print(f"Saving model to {output_path}...")
    with open(output_path, 'wb') as f:
        pickle.dump(model, f)

    print("✅ Congestion model trained successfully!")
    return model

if __name__ == "__main__":
    import sys
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "inputfiles/SanDiegoFCD1k.h5"
    train_congestion_model(h5_file)
