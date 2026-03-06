import json
import time
import argparse
import h5py
import pandas as pd


def row_to_message(row):
    node_from = int(row["node_from"])
    node_to = int(row["node_to"])
    segment_id = f"{node_from}_{node_to}"

    msg = {
        "schema_version": "ruth.fcd.v1",
        "event_type": "fcd_update",
        "timestamp": int(row["timestamp"]),
        "vehicle": {
            "id": int(row["vehicle_id"]),
            "type": str(row["vehicle_type"]),
            "active": bool(row["active"]),
        },
        "road": {
            "node_from": node_from,
            "node_to": node_to,
            "segment_id": segment_id,
            "segment_length_m": float(row["segment_length"]),
        },
        "motion": {
            "offset_m": float(row["start_offset_m"]),
            "speed_mps": float(row["speed_mps"]),
        },
    }
    return msg


def load_fcd_df(h5_path: str, dataset_key: str = "fcd") -> pd.DataFrame:
    with h5py.File(h5_path, "r") as f:
        if dataset_key not in f:
            raise KeyError(f"Dataset key '{dataset_key}' not found. Available keys: {list(f.keys())}")
        data = f[dataset_key][:]

    df = pd.DataFrame.from_records(data)

    # Decode bytes -> string for vehicle_type (e.g., b"truck" -> "truck")
    if "vehicle_type" in df.columns:
        df["vehicle_type"] = df["vehicle_type"].apply(
            lambda x: x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)
        )

    return df


def stream_fcd(df, replay_mode=False, speedup=1.0, limit=None):
    df = df.sort_values("timestamp")

    if replay_mode and speedup <= 0:
        raise ValueError("speedup must be > 0 when replay_mode is enabled")

    previous_timestamp = None
    sent = 0

    for _, row in df.iterrows():
        current_timestamp = int(row["timestamp"])

        if replay_mode and previous_timestamp is not None:
            time_gap = current_timestamp - previous_timestamp
            if time_gap > 0:
                time.sleep(time_gap / speedup)

        msg = row_to_message(row)
        print(json.dumps(msg))

        previous_timestamp = current_timestamp
        sent += 1

        if limit is not None and sent >= limit:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream RUTH FCD (.h5) rows as JSON messages")
    parser.add_argument("--h5", required=True, help="Path to FCD H5 file")
    parser.add_argument("--key", default="fcd", help="Dataset key inside the H5 file (default: fcd)")
    parser.add_argument("--replay", action="store_true", help="Replay with time delays based on timestamp gaps")
    parser.add_argument("--speedup", type=float, default=10.0, help="Replay speed multiplier (bigger = faster)")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N messages (useful for quick tests)")

    args = parser.parse_args()

    df = load_fcd_df(args.h5, dataset_key=args.key)
    try:
        stream_fcd(df, replay_mode=args.replay, speedup=args.speedup, limit=args.limit)
    except BrokenPipeError:
        pass