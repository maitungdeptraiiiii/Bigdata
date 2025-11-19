import csv
import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime

CSV_FILE = os.environ.get("CSV_FILE", "/data/online_gaming_behavior_dataset.csv")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)

EVENT_TYPES = ["login", "logout", "purchase", "level_up", "achievement_unlocked"]

def random_event(player):
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.3, 0.25, 0.1, 0.2, 0.15],
        k=1
    )[0]

    base_event = {
        "event_id": f"{player['PlayerID']}_{int(time.time() * 1000)}_{random.randint(100,999)}",
        "player_id": int(player["PlayerID"]),
        "timestamp": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
        "event_type": event_type,
    }

    if event_type == "login":
        base_event["location"] = player.get("Location", "")
        base_event["device"] = random.choice(["PC", "Mobile", "Console"])
    elif event_type == "logout":
        base_event["session_duration"] = random.randint(5, 300)
    elif event_type == "purchase":
        base_event["item"] = random.choice(["Skin", "Weapon", "Bundle", "Boost"])
        base_event["amount"] = round(random.uniform(1.99, 49.99), 2)
    elif event_type == "level_up":
        try:
            new_level = int(player.get("PlayerLevel", 1)) + random.randint(1, 3)
        except:
            new_level = 1
        base_event["new_level"] = new_level
    elif event_type == "achievement_unlocked":
        base_event["achievement"] = random.choice([
            "First Blood", "Sharp Shooter", "Marathon Runner",
            "Collector", "Unstoppable", "Master Strategist"
        ])

    return base_event

def main():
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        players = [row for row in reader]
        print(f"Loaded {len(players)} players, start streaming to {BOOTSTRAP}...")
        while True:
            player = random.choice(players)
            event = random_event(player)
            producer.send(TOPIC, event)
            print("Sent:", event["event_type"], "-", event["event_id"])
            time.sleep(random.uniform(0.05, 0.3))

if __name__ == "__main__":
    main()
