import os
import time
import sys

# Bắt buộc flush stdout để log hiện ra ngay lập tức trong Docker
sys.stdout.reconfigure(line_buffering=True)

print(">>> PRODUCER SCRIPT STARTING...", flush=True)

try:
    import csv
    import json
    import random
    from datetime import datetime
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError as e:
    print(f"CRITICAL ERROR: Missing libraries. {e}", flush=True)
    # Ngủ để container không restart, giúp bạn đọc được log lỗi
    while True: time.sleep(3600)

# --- CẤU HÌNH ---
CSV_FILE = os.environ.get("CSV_FILE", "/data/online_gaming_behavior_dataset.csv")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")
DEMO_PLAYER_COUNT = 20

EVENT_TYPES = ["login", "logout", "purchase", "level_up", "achievement_unlocked"]

def get_timestamp():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def create_producer():
    print(f"Connecting to Kafka at {BOOTSTRAP}...", flush=True)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(">>> Successfully connected to Kafka!", flush=True)
            return producer
        except NoBrokersAvailable:
            print(">>> Kafka is not ready yet. Retrying in 3 seconds...", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f">>> Unexpected error connecting to Kafka: {e}", flush=True)
            time.sleep(3)

def send_event(producer, event_data):
    try:
        producer.send(TOPIC, event_data)
        print(f"Sent: {event_data['event_type']} | Player: {event_data['player_id']}", flush=True)
    except Exception as e:
        print(f"Error sending event: {e}", flush=True)

def main():
    print(">>> Entering Main Function...", flush=True)
    
    # 1. Kết nối Kafka
    producer = create_producer()
    print(">>> Producer connected. Waiting 10s for Spark & WebUI to fully start...", flush=True)
    time.sleep(10)

    # 2. Đọc CSV
    all_rows = []
    print(f"Loading players from {CSV_FILE}...", flush=True)
    
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
    else:
        print(f"ERROR: File {CSV_FILE} not found! Checking current dir...", flush=True)
        print(os.listdir("/"), flush=True)
        # Không return mà giữ process sống để debug
        while True: time.sleep(10)

    total_in_csv = len(all_rows)
    print(f"Found {total_in_csv} players.", flush=True)

    # 3. Lấy mẫu
    if total_in_csv > DEMO_PLAYER_COUNT:
        players = random.sample(all_rows, DEMO_PLAYER_COUNT)
    else:
        players = all_rows
    
    print(f"Simulation started for {len(players)} players.", flush=True)

    # 4. Gửi Register
    print("Sending REGISTER events...", flush=True)
    for p in players:
        register_event = {
            "event_id": f"reg_{p['PlayerID']}",
            "event_type": "register",
            "player_id": p['PlayerID'],
            "timestamp": get_timestamp(),
            "Age": int(p.get('Age', 0)),
            "Gender": p.get('Gender', 'Unknown'),
            "Location": p.get('Location', 'Unknown'),
            "GameGenre": p.get('GameGenre', 'Unknown'),
            "GameDifficulty": p.get('GameDifficulty', 'Medium'),
            "status": "offline"
        }
        send_event(producer, register_event)
        time.sleep(0.05) 

    print(">>> Realtime simulation loop starting...", flush=True)

    # 5. Vòng lặp vô tận
    while True:
        player = random.choice(players)
        player_id = player['PlayerID']
        
        event_type = random.choices(
            EVENT_TYPES, 
            weights=[0.25, 0.25, 0.1, 0.2, 0.2]
        )[0]

        event = {
            "event_id": f"evt_{int(time.time()*1000)}",
            "player_id": player_id,
            "event_type": event_type,
            "timestamp": get_timestamp(),
        }

        if event_type == "purchase":
            event["item_id"] = f"item_{random.randint(1, 100)}"
            event["amount"] = round(random.uniform(0.99, 99.99), 2)
        elif event_type == "level_up":
            event["new_level"] = random.randint(1, 100)
        
        send_event(producer, event)
        
        # Sleep ngẫu nhiên
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"CRITICAL UNHANDLED EXCEPTION: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # Giữ container sống để đọc log
        print("Producer died unexpectedly. Sleeping forever to preserve logs...", flush=True)
        while True: time.sleep(3600)