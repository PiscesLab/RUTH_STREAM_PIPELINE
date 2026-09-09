#!/usr/bin/env python3
"""
Train the ML models for the RUTH traffic pipeline.

Two models, both predicting something that cannot be computed from their own
inputs:

  1. Travel time      - how long a vehicle actually takes to cross a segment
  2. Future traffic   - the speed on a segment 5 minutes from now, which the
                        pipeline thresholds into HIGH/MEDIUM/LOW

Current congestion is not a model. It is a threshold on the current average
speed, so the pipeline computes it directly. A classifier trained on it scored
100% while doing nothing a three-line rule does exactly - see the README.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from train_future_congestion_model import train_future_congestion_model
from train_travel_time_model import train_travel_time_model


def main():
    h5_file = sys.argv[1] if len(sys.argv) > 1 else "../inputfiles/SanDiegoFCD100.h5"

    if not os.path.exists(h5_file):
        print(f"❌ Error: H5 file not found at {h5_file}")
        sys.exit(1)

    print("=" * 60)
    print("🚦 Training ML Models for RUTH Traffic Pipeline")
    print("=" * 60)
    print()

    try:
        print("Step 1/2: Travel Time (actual crossing duration)...")
        print("-" * 60)
        train_travel_time_model(h5_file)
        print()

        print("Step 2/2: Future Traffic (speed 5 minutes ahead)...")
        print("-" * 60)
        train_future_congestion_model(h5_file)
        print()

        print("=" * 60)
        print("✅ All models trained successfully!")
        print("=" * 60)
        print()
        print("Models saved to ml/models/:")
        print("  - travel_time_model.pkl")
        print("  - future_congestion_model.pkl")
        print()
        print("Current congestion needs no model: it is a threshold on the")
        print("current window's average speed, applied directly in the pipeline.")

    except Exception as e:
        print(f"❌ Error during training: {e}")
        raise


if __name__ == "__main__":
    main()
