import csv
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

CSV_FILE = "../data/online_gaming_behavior_dataset.csv"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

EVENT_TYPES = ["login", "logout", "purchase", "level_up", "achievement_unlocked"]

def random_event(player):
    """Sinh ngẫu nhiên 1 event dựa trên thông tin người chơi"""
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.3, 0.25, 0.1, 0.2, 0.15],  # tần suất
        k=1
    )[0]

    base_event = {
        "event_id": f"{player['PlayerID']}_{int(time.time() * 1000)}_{random.randint(100,999)}",
        "player_id": int(player["PlayerID"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event_type": event_type,
    }

    # Bổ sung dữ liệu tùy loại event
    if event_type == "login":
        base_event["location"] = player["Location"]
        base_event["device"] = random.choice(["PC", "Mobile", "Console"])

    elif event_type == "logout":
        base_event["session_duration"] = random.randint(5, 300)

    elif event_type == "purchase":
        base_event["item"] = random.choice(["Skin", "Weapon", "Bundle", "Boost"])
        base_event["amount"] = round(random.uniform(1.99, 49.99), 2)

    elif event_type == "level_up":
        new_level = int(player["PlayerLevel"]) + random.randint(1, 3)
        base_event["new_level"] = new_level

    elif event_type == "achievement_unlocked":
        base_event["achievement"] = random.choice([
            "First Blood", "Sharp Shooter", "Marathon Runner",
            "Collector", "Unstoppable", "Master Strategist"
        ])

    return base_event


# ---------------- RUN STREAM PRODUCER ----------------
with open(CSV_FILE, encoding="utf-8") as f:
    reader = csv.DictReader(f)

    players = [row for row in reader]   # load toàn bộ dataset

    print(f"Loaded {len(players)} players, start streaming...")

    while True:
        # chọn 1 người chơi bất kỳ
        player = random.choice(players)

        # sinh event
        event = random_event(player)

        # gửi Kafka
        producer.send("game_events", event)

        print("Sent:", event["event_type"], "-", event["event_id"])

        # tốc độ stream
        time.sleep(random.uniform(0.05, 0.3))
