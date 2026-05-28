import requests
import time
import datetime
import json
from kafka import KafkaProducer

# ── CONFIG ────────────────────────────────────────────────────────────────────
TOMTOM_API_KEY = "HAcMB5quccEOYKWmHpFGNkdTnNiEkGmN"
TOMTOM_URL     = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
POLL_INTERVAL  = 10
API_DELAY      = 0.5

# ── CLASSIFICATION THRESHOLDS ─────────────────────────────────────────────────
# Indian urban roads run slow by nature. We combine two signals:
#   1. Speed ratio  — how much slower than normal (relative congestion)
#   2. Absolute speed — raw km/h (catches slow-by-nature roads in jams)
#
# A location is HIGH if EITHER condition is true (more sensitive).
# A location is LOW  only if BOTH conditions are comfortable.

RATIO_HIGH    = 0.60   # below 60% of normal → congested
RATIO_MEDIUM  = 0.80   # below 80% of normal → moderate

ABS_HIGH_KMH  = 12     # below 12 km/h absolute → HIGH regardless of ratio
ABS_MED_KMH   = 20     # below 20 km/h absolute → at least MEDIUM

# ── ALL 15 HYDERABAD LOCATIONS ────────────────────────────────────────────────
LOCATIONS = [
    ("Ameerpet",     "17.4375,78.4482"),
    ("Gachibowli",   "17.4401,78.3489"),
    ("Kukatpally",   "17.4948,78.3996"),
    ("Secunderabad", "17.4399,78.4983"),
    ("Miyapur",      "17.4969,78.3562"),
    ("Madhapur",     "17.4483,78.3915"),
    ("BanjaraHills", "17.4126,78.4482"),
    ("JubileeHills", "17.4239,78.4738"),
    ("LBNagar",      "17.3457,78.5522"),
    ("Dilsukhnagar", "17.3688,78.5247"),
    ("Charminar",    "17.3616,78.4747"),
    ("Mehdipatnam",  "17.3950,78.4330"),
    ("Begumpet",     "17.4448,78.4667"),
    ("HitechCity",   "17.4435,78.3772"),
    ("Uppal",        "17.4050,78.5591"),
]

# ── KAFKA ─────────────────────────────────────────────────────────────────────
def create_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers="localhost:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                retry_backoff_ms=1000,
            )
            print("✅ Connected to Kafka.")
            return p
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

# ── CLASSIFICATION ────────────────────────────────────────────────────────────
def classify(current_speed: float, free_flow_speed: float) -> tuple:
    """
    Combined ratio + absolute speed classification.

    Why both?
    - Ratio alone fails when freeFlowSpeed is already very low (e.g. Charminar 14 km/h).
      Even in a jam, ratio stays ~1.0.
    - Absolute speed alone misses congestion on fast arterial roads where
      40 km/h feels normal but 30 km/h is actually congested.
    - Using BOTH catches all cases.

    Returns: (ratio, congestion_level, estimated_vehicles)
    """
    ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 1.0
    ratio = max(0.0, min(1.0, ratio))

    # Determine level using whichever signal shows worse congestion
    if current_speed < ABS_HIGH_KMH or ratio < RATIO_HIGH:
        level = "HIGH"
    elif current_speed < ABS_MED_KMH or ratio < RATIO_MEDIUM:
        level = "MEDIUM"
    else:
        level = "LOW"

    # Vehicles: inverse of ratio, range 50–500
    vehicles = int(50 + (1 - ratio) * 450)

    return round(ratio, 3), level, vehicles

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    producer = create_producer()

    while True:
        now = datetime.datetime.now()

        for name, point in LOCATIONS:
            try:
                res = requests.get(
                    TOMTOM_URL,
                    params={"point": point, "key": TOMTOM_API_KEY},
                    timeout=10,
                )
                res.raise_for_status()

                seg          = res.json()["flowSegmentData"]
                current_spd  = seg["currentSpeed"]
                freeflow_spd = seg["freeFlowSpeed"]
                confidence   = seg.get("confidence", 1.0)

                ratio, level, vehicles = classify(current_spd, freeflow_spd)

                message = {
                    "location":         name,
                    "vehicles":         vehicles,
                    "congestion_level": level,
                    "current_speed":    current_spd,
                    "freeflow_speed":   freeflow_spd,
                    "ratio":            ratio,
                    "confidence":       confidence,
                    "timestamp":        now.isoformat(),
                }

                producer.send("traffic", message)
                print(
                    f"[{now.strftime('%H:%M:%S')}] {name:<15} | "
                    f"{current_spd:>3}/{freeflow_spd:<3} km/h | "
                    f"ratio {ratio:.2f} | {level:<6} | ~{vehicles} vehicles"
                )

            except requests.exceptions.HTTPError as e:
                print(f"[HTTP ERROR] {name}: {e}")
            except KeyError as e:
                print(f"[PARSE ERROR] {name}: missing key {e}")
            except Exception as e:
                print(f"[ERROR] {name}: {e}")

            time.sleep(API_DELAY)

        try:
            producer.flush()
        except Exception as e:
            print(f"[FLUSH ERROR] {e} — reconnecting...")
            producer = create_producer()

        print(f"--- sweep done, sleeping {POLL_INTERVAL}s ---\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()