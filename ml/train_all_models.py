#!/usr/bin/env python3
"""
Train all ML models for the RUTH traffic pipeline
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from train_congestion_model import train_congestion_model
from train_travel_time_model import train_travel_time_model
from train_future_congestion_model import train_future_congestion_model

def main():
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD1k.h5"

    if not os.path.exists(h5_file):
        print(f"❌ Error: H5 file not found at {h5_file}")
        sys.exit(1)

    print("="*60)
    print("🚦 Training All ML Models for RUTH Traffic Pipeline")
    print("="*60)
    print()

    try:
        print("Step 1/3: Training Congestion Prediction Model...")
        print("-" * 60)
        train_congestion_model(h5_file)
        print()

        print("Step 2/3: Training Travel Time Prediction Model...")
        print("-" * 60)
        train_travel_time_model(h5_file)
        print()

        print("Step 3/3: Training Future Congestion Prediction Model...")
        print("-" * 60)
        train_future_congestion_model(h5_file)
        print()

        print("="*60)
        print("✅ All models trained successfully!")
        print("="*60)
        print()
        print("Models saved to ml/models/:")
        print("  - congestion_model.pkl")
        print("  - travel_time_model.pkl")
        print("  - future_congestion_model.pkl")
        print()
        print("Ready to run the pipeline with ML predictions!")

    except Exception as e:
        print(f"❌ Error during training: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
