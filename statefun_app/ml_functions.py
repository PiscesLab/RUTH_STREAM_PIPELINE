import pickle
import os
import numpy as np

# Global model cache
_models = None

def load_models():
    """Load trained ML models from pickle files"""
    global _models
    if _models is not None:
        return _models

    ml_dir = os.path.join(os.path.dirname(__file__), '..', 'ml', 'models')

    congestion_path = os.path.join(ml_dir, 'congestion_model.pkl')
    travel_time_path = os.path.join(ml_dir, 'travel_time_model.pkl')
    future_congestion_path = os.path.join(ml_dir, 'future_congestion_model.pkl')

    try:
        with open(congestion_path, 'rb') as f:
            congestion_model = pickle.load(f)
        with open(travel_time_path, 'rb') as f:
            travel_time_model = pickle.load(f)
        with open(future_congestion_path, 'rb') as f:
            future_congestion_model = pickle.load(f)

        _models = {
            'congestion': congestion_model,
            'travel_time': travel_time_model,
            'future_congestion': future_congestion_model
        }

        print("✅ ML models loaded successfully")
        return _models
    except FileNotFoundError as e:
        print(f"⚠️  Warning: Could not load models - {e}")
        return None

def predict_congestion(avg_speed, max_speed, min_speed, std_speed, vehicle_count,
                       observation_count):
    """Predict congestion level using trained model"""
    models = load_models()
    if models is None:
        return None

    try:
        import pandas as pd
        features = pd.DataFrame({
            'avg_speed': [avg_speed],
            'max_speed': [max_speed],
            'min_speed': [min_speed],
            'std_speed': [std_speed],
            'vehicle_count': [vehicle_count],
            'observation_count': [observation_count]
        })
        prediction = models['congestion'].predict(features)[0]
        return prediction
    except Exception as e:
        return None

def predict_travel_time(segment_length, avg_speed, max_speed, vehicle_count,
                        observation_count):
    """Predict travel time using trained model"""
    models = load_models()
    if models is None:
        return None

    try:
        import pandas as pd
        features = pd.DataFrame({
            'segment_length': [segment_length],
            'avg_speed': [avg_speed],
            'max_speed': [max_speed],
            'vehicle_count': [vehicle_count],
            'observation_count': [observation_count]
        })
        prediction = models['travel_time'].predict(features)[0]
        return max(0, prediction)  # Ensure positive travel time
    except Exception as e:
        return None

def predict_future_congestion(avg_speed, max_speed, min_speed, std_speed,
                              vehicle_count, observation_count,
                              vehicle_type_diversity, current_congestion):
    """Predict future congestion level using trained model"""
    models = load_models()
    if models is None:
        return None

    try:
        import pandas as pd
        features = pd.DataFrame({
            'avg_speed': [avg_speed],
            'max_speed': [max_speed],
            'min_speed': [min_speed],
            'std_speed': [std_speed],
            'vehicle_count': [vehicle_count],
            'observation_count': [observation_count],
            'vehicle_type_diversity': [vehicle_type_diversity],
            'current_congestion': [current_congestion]
        })
        prediction = models['future_congestion'].predict(features)[0]
        return prediction
    except Exception as e:
        return None

def get_ml_predictions(segment_id, segment_length, count, observation_count,
                       avg_speed, max_speed, min_speed, std_speed,
                       vehicle_type_diversity=1, current_congestion_level=1):
    """
    Get all ML predictions for a segment from the statistics observed in its
    current time window (tracked as StateFun state in segment_fn).

    Args:
        segment_id: Unique segment identifier
        segment_length: Length of road segment in meters
        count: Distinct vehicles seen in the window
        observation_count: FCD samples in the window (a vehicle is sampled
            repeatedly while it crosses, so this runs well above `count`)
        avg_speed: Mean of observed speeds (m/s)
        max_speed: Max of observed speeds (m/s)
        min_speed: Min of observed speeds (m/s)
        std_speed: Standard deviation of observed speeds (m/s)
        vehicle_type_diversity: Number of different vehicle types
        current_congestion_level: 0=HIGH, 1=MEDIUM, 2=LOW

    Returns:
        dict with predictions or None if models not available
    """
    if observation_count == 0 or avg_speed == 0:
        return None

    try:
        congestion_pred = predict_congestion(
            avg_speed, max_speed, min_speed, std_speed, count, observation_count
        )

        travel_time_pred = predict_travel_time(
            segment_length, avg_speed, max_speed, count, observation_count
        )

        future_congestion_pred = predict_future_congestion(
            avg_speed, max_speed, min_speed, std_speed,
            count, observation_count, vehicle_type_diversity,
            current_congestion_level
        )

        predictions = {}
        if congestion_pred:
            predictions['predicted_congestion'] = congestion_pred
        if travel_time_pred is not None:
            predictions['predicted_travel_time'] = f"{travel_time_pred:.2f}"
        if future_congestion_pred:
            predictions['predicted_next_congestion'] = future_congestion_pred

        return predictions if predictions else None

    except Exception as e:
        print(f"Error generating ML predictions: {e}")
        return None
