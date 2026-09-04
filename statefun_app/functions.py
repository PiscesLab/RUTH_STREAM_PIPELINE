from aiohttp import web
from statefun import *
import json
from ml_functions import get_ml_predictions

functions = StatefulFunctions()


@functions.bind(
    typename="com.ruth/vehicle",
    specs=[ValueSpec(name="last_speed", type=DoubleType)],
)
async def vehicle_fn(ctx: Context, message: Message):
    # Kafka ingress payload arrives as raw bytes
    raw_bytes = message.raw_value()
    raw = raw_bytes.decode("utf-8")
    event = json.loads(raw)

    speed = float(event["motion"]["speed_mps"])
    segment_id = event["road"]["segment_id"]
    segment_length = float(event["road"]["segment_length_m"])
    vehicle_type = event["vehicle"]["type"]

    ctx.storage.last_speed = speed

    out = {
        "segment_id": segment_id,
        "speed": speed,
        "timestamp": event["timestamp"],
        "vehicle_id": event["vehicle"]["id"],
        "segment_length_m": segment_length,
        "vehicle_type": vehicle_type,
    }

    ctx.send(
        message_builder(
            target_typename="com.ruth/segment",
            target_id=segment_id,
            str_value=json.dumps(out),
        )
    )


@functions.bind(
    typename="com.ruth/segment",
    specs=[
        ValueSpec(name="count", type=IntType),
        ValueSpec(name="speed_sum", type=DoubleType),
        ValueSpec(name="segment_length_m", type=DoubleType),
        ValueSpec(name="vehicle_types", type=StringType),
    ],
)
async def segment_fn(ctx: Context, message: Message):
    raw = message.as_string()
    data = json.loads(raw)

    count = ctx.storage.count or 0
    speed_sum = ctx.storage.speed_sum or 0.0

    count += 1
    speed_sum += float(data["speed"])

    # track the distinct vehicle types observed on this segment so far
    seen_types = set(filter(None, (ctx.storage.vehicle_types or "").split(",")))
    seen_types.add(data.get("vehicle_type", "unknown"))
    vehicle_type_diversity = len(seen_types)

    # keep latest known segment length in state
    ctx.storage.segment_length_m = float(data["segment_length_m"])
    ctx.storage.count = count
    ctx.storage.speed_sum = speed_sum
    ctx.storage.vehicle_types = ",".join(sorted(seen_types))

    avg_speed = speed_sum / count if count else 0.0

    if avg_speed < 5:
        congestion = "HIGH"
    elif avg_speed < 12:
        congestion = "MEDIUM"
    else:
        congestion = "LOW"

    print(
        f"segment={data['segment_id']} "
        f"count={count} avg_speed={avg_speed:.2f} congestion={congestion} "
        f"vehicle_types={ctx.storage.vehicle_types}"
    )

    # Get ML predictions
    congestion_level = 0 if congestion == "HIGH" else (1 if congestion == "MEDIUM" else 2)
    ml_preds = get_ml_predictions(
        data['segment_id'],
        float(ctx.storage.segment_length_m or 0.0),
        count,
        speed_sum,
        vehicle_type_diversity=vehicle_type_diversity,
        current_congestion_level=congestion_level
    )
    if ml_preds and 'predicted_congestion' in ml_preds:
        print(f"[CONGESTION PREDICTION] segment={data['segment_id']} predicted_congestion={ml_preds['predicted_congestion']}")

    travel_msg = {
        "segment_id": data["segment_id"],
        "avg_speed_mps": avg_speed,
        "segment_length_m": float(ctx.storage.segment_length_m or 0.0),
        "timestamp": data["timestamp"],
        "vehicle_count": count,
        "current_congestion_level": congestion_level,
        "vehicle_type_diversity": vehicle_type_diversity,
    }

    ctx.send(
        message_builder(
            target_typename="com.ruth/travel_time",
            target_id=data["segment_id"],
            str_value=json.dumps(travel_msg),
        )
    )


@functions.bind(
    typename="com.ruth/travel_time",
    specs=[],
)
async def travel_time_fn(ctx: Context, message: Message):
    raw = message.as_string()
    data = json.loads(raw)

    avg_speed = float(data["avg_speed_mps"])
    segment_length = float(data["segment_length_m"])

    if avg_speed > 0:
        travel_time_seconds = segment_length / avg_speed
    else:
        travel_time_seconds = 0.0

    print(
        f"[TRAVEL TIME] segment={data['segment_id']} "
        f"length={segment_length:.2f}m "
        f"avg_speed={avg_speed:.2f}m/s "
        f"time={travel_time_seconds:.2f}s"
    )

    # Get ML predictions for travel time and future congestion, using the
    # real per-segment state computed in segment_fn (not fabricated values)
    try:
        vehicle_count = int(data.get("vehicle_count", 1)) or 1
        vehicle_type_diversity = int(data.get("vehicle_type_diversity", 1))
        current_congestion_level = int(data.get("current_congestion_level", 1))

        ml_preds = get_ml_predictions(
            data['segment_id'],
            segment_length,
            vehicle_count,
            avg_speed * vehicle_count,
            vehicle_type_diversity=vehicle_type_diversity,
            current_congestion_level=current_congestion_level
        )

        if ml_preds:
            if 'predicted_travel_time' in ml_preds:
                print(f"[TRAVEL TIME PREDICTION] segment={data['segment_id']} predicted_travel_time={ml_preds['predicted_travel_time']}s")
            if 'predicted_next_congestion' in ml_preds:
                print(f"[FUTURE CONGESTION PREDICTION] segment={data['segment_id']} predicted_next_congestion={ml_preds['predicted_next_congestion']}")
    except Exception as e:
        pass  # Silent fail if ML not available


handler = RequestReplyHandler(functions)


async def handle(request):
    body = await request.read()
    response = await handler.handle_async(body)
    return web.Response(body=response, content_type="application/octet-stream")


app = web.Application()
app.add_routes([web.post("/statefun", handle)])


if __name__ == "__main__":
    web.run_app(app, port=8000)
