"""Online inference for the RUTH pipeline.

Two models, both predicting something not derivable from their inputs:

  travel_time       - how long a vehicle will actually take to cross, from what
                      is known as it enters (length, its speed, its type, and
                      the traffic already on the segment)
  future_congestion - the segment's mean speed HORIZON seconds from now; the
                      HIGH/MEDIUM/LOW class is derived by thresholding it

Current congestion is not a model - it is a threshold on the current window's
average speed and is computed directly in functions.py.

Feature order and meaning must match ml/features.py, which defines what the
models were trained on.
"""

import os
import pickle

# Speed thresholds, mirroring congestion_label() in ml/features.py
HIGH_BELOW = 5.0
MEDIUM_BELOW = 12.0

# Feature order expected by each model, matching the training scripts.
TRAVEL_TIME_FEATURES = [
    "segment_length",
    "entry_speed",
    "is_truck",
    "segment_avg_speed",
    "segment_samples",
]

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

_models = None


def congestion_label(avg_speed):
    """HIGH / MEDIUM / LOW for a given speed."""
    if avg_speed < HIGH_BELOW:
        return "HIGH"
    if avg_speed < MEDIUM_BELOW:
        return "MEDIUM"
    return "LOW"


def load_models():
    """Load trained models from disk, once per process."""
    global _models
    if _models is not None:
        return _models

    ml_dir = os.path.join(os.path.dirname(__file__), "..", "ml", "models")

    try:
        with open(os.path.join(ml_dir, "travel_time_model.pkl"), "rb") as f:
            travel_time_model = pickle.load(f)
        with open(os.path.join(ml_dir, "future_congestion_model.pkl"), "rb") as f:
            future_model = pickle.load(f)

        _models = {"travel_time": travel_time_model, "future_congestion": future_model}
        print("✅ ML models loaded successfully")
        return _models
    except FileNotFoundError as e:
        print(f"⚠️  Warning: Could not load models - {e}")
        print("   Train them first: python3 ml/train_all_models.py <file.h5>")
        return None


def _predict(model, feature_names, values):
    import pandas as pd

    frame = pd.DataFrame({name: [values[name]] for name in feature_names})
    return model.predict(frame)[0]


def predict_travel_time(segment_length, entry_speed, is_truck,
                        segment_avg_speed, segment_samples):
    """Seconds for the current vehicle to cross this segment."""
    models = load_models()
    if models is None:
        return None
    try:
        value = _predict(
            models["travel_time"],
            TRAVEL_TIME_FEATURES,
            {
                "segment_length": segment_length,
                "entry_speed": entry_speed,
                "is_truck": int(is_truck),
                "segment_avg_speed": segment_avg_speed,
                "segment_samples": segment_samples,
            },
        )
        return max(0.0, float(value))
    except Exception:
        return None


def predict_future_speed(avg_speed, max_speed, min_speed, std_speed,
                         vehicle_count, observation_count,
                         vehicle_type_diversity, segment_length):
    """Mean speed (m/s) expected on this segment over the next horizon."""
    models = load_models()
    if models is None:
        return None
    try:
        value = _predict(
            models["future_congestion"],
            FUTURE_FEATURES,
            {
                "avg_speed": avg_speed,
                "max_speed": max_speed,
                "min_speed": min_speed,
                "std_speed": std_speed,
                "vehicle_count": vehicle_count,
                "observation_count": observation_count,
                "vehicle_type_diversity": vehicle_type_diversity,
                "segment_length": segment_length,
            },
        )
        return max(0.0, float(value))
    except Exception:
        return None


def get_ml_predictions(segment_id, segment_length, vehicle_count,
                       observation_count, avg_speed, max_speed, min_speed,
                       std_speed, entry_speed=None, is_truck=0,
                       vehicle_type_diversity=1):
    """Run both models for a segment.

    Args:
        segment_id: Unique segment identifier
        segment_length: Length of road segment in metres
        vehicle_count: Distinct vehicles in the current window
        observation_count: FCD samples in the current window
        avg_speed / max_speed / min_speed / std_speed: window speed statistics
        entry_speed: speed of the vehicle that just arrived (defaults to
            avg_speed when not supplied)
        is_truck: 1 if that vehicle is a truck
        vehicle_type_diversity: distinct vehicle types in the window

    Returns:
        dict of predictions, or None if the models are unavailable.
    """
    if observation_count == 0 or avg_speed == 0:
        return None

    if entry_speed is None:
        entry_speed = avg_speed

    predictions = {}

    travel_time = predict_travel_time(
        segment_length, entry_speed, is_truck, avg_speed, observation_count
    )
    if travel_time is not None:
        predictions["predicted_travel_time"] = f"{travel_time:.2f}"

    future_speed = predict_future_speed(
        avg_speed, max_speed, min_speed, std_speed, vehicle_count,
        observation_count, vehicle_type_diversity, segment_length,
    )
    if future_speed is not None:
        predictions["predicted_future_speed"] = f"{future_speed:.2f}"
        predictions["predicted_next_congestion"] = congestion_label(future_speed)

    return predictions or None
