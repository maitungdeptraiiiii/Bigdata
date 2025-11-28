import csv
import json
import time
import random
import os
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# --- CẤU HÌNH ---
CSV_FILE = os.environ.get("CSV_FILE", "/data/online_gaming_behavior_dataset.csv")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")

TARGET_RPS = 100
SLEEP_TIME = 1.0 / TARGET_RPS
SAMPLES_PER_GROUP = 50 

sys.stdout.reconfigure(line_buffering=True)

class PlayerAgent:
    def __init__(self, profile):
        self.profile = profile
        self.player_id = profile['PlayerID']
        self.is_online = False
        self.current_level = int(profile.get('PlayerLevel', 1))
        
        try:
            self.avg_duration = float(profile.get('AvgSessionDurationMinutes', 30))
            self.sessions_per_week = float(profile.get('SessionsPerWeek', 5))
            self.engagement_group = profile.get('EngagementLevel', 'Medium')
        except:
            self.avg_duration = 30.0
            self.sessions_per_week = 5.0
            self.engagement_group = 'Medium'

        # FIX: Đảm bảo login_weight luôn dương
        self.login_weight = self.sessions_per_week + 0.1

    def get_action_probabilities(self):
        if self.engagement_group == 'High':
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.6, 0.15, 0.05, 0.1, 0.1]
        elif self.engagement_group == 'Medium':
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.6, 0.05, 0.05, 0.1, 0.2]
        else: 
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.5, 0.01, 0.02, 0.02, 0.45]

    def generate_event(self, event_type):
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        base_event = {
            "event_id": f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}",
            "player_id": self.player_id,
            "event_type": event_type,
            "timestamp": timestamp
        }

        if event_type == "login":
            self.is_online = True
        elif event_type == "logout":
            self.is_online = False
            duration = random.normalvariate(self.avg_duration, self.avg_duration/4)
            duration = max(1.0, min(duration, 300.0)) 
            base_event["session_duration"] = round(duration, 2)
        elif event_type == "purchase":
            amt = random.uniform(4.99, 99.99) if self.engagement_group == 'High' else random.uniform(0.99, 9.99)
            base_event["item_id"] = f"item_{random.randint(1, 100)}"
            base_event["amount"] = round(amt, 2)
        elif event_type == "level_up":
            self.current_level += 1
            base_event["new_level"] = self.current_level

        return base_event

def load_stratified_players():
    print(f"Loading players from {CSV_FILE}...", flush=True)
    if not os.path.exists(CSV_FILE): return []

    high, med, low = [], [], []
    with open(CSV_FILE, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            lv = row.get('EngagementLevel', 'Medium')
            if lv == 'High': high.append(row)
            elif lv == 'Medium': med.append(row)
            else: low.append(row)

    def safe_sample(pop, k): return random.sample(pop, min(len(pop), k))
    selected = safe_sample(high, SAMPLES_PER_GROUP) + safe_sample(med, SAMPLES_PER_GROUP) + safe_sample(low, SAMPLES_PER_GROUP)
    random.shuffle(selected)
    return selected

def create_producer():
    print(f"Connecting to Kafka at {BOOTSTRAP}...", flush=True)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                linger_ms=10, 
                batch_size=16384
            )
            print(">>> Kafka Connected!", flush=True)
            return producer
        except NoBrokersAvailable:
            print(">>> Kafka not ready. Retrying...", flush=True)
            time.sleep(2)

def main():
    producer = create_producer()
    print(">>> Waiting 5s...", flush=True)
    time.sleep(5)

    profiles = load_stratified_players()
    if not profiles: return
    agents = [PlayerAgent(p) for p in profiles]
    print(f">>> Initialized {len(agents)} agents. Target Speed: {TARGET_RPS} events/s", flush=True)

    print(">>> Sending Registers...", flush=True)
    for i, ag in enumerate(agents):
        reg = {
            "event_type": "register",
            "player_id": ag.player_id,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "Age": int(ag.profile.get('Age', 0)),
            "Gender": ag.profile.get('Gender', 'Unknown'),
            "Location": ag.profile.get('Location', 'Unknown'),
            "GameGenre": ag.profile.get('GameGenre', 'Unknown'),
            "GameDifficulty": ag.profile.get('GameDifficulty', 'Medium'),
            "EngagementLevel": ag.engagement_group,
            "status": "offline"
        }
        producer.send(TOPIC, key=ag.player_id, value=reg)
        if i % 10 == 0: time.sleep(0.01)
    
    producer.flush()
    print(">>> Registration Done. Starting Loop...", flush=True)
    time.sleep(2)

    count = 0
    start_time = time.time()

    while True:
        loop_start = time.time()

        online = [a for a in agents if a.is_online]
        offline = [a for a in agents if not a.is_online]
        
        mode = "login"
        if online and random.random() > 0.3: mode = "action"
        if not offline: mode = "action"
        if not online: mode = "login"

        agent = None
        action = None

        if mode == "login":
            w = [a.login_weight for a in offline]
            # FIX: Tránh lỗi sum(weights) = 0
            if sum(w) <= 0:
                agent = random.choice(offline)
            else:
                agent = random.choices(offline, weights=w, k=1)[0]
            action = "login"
        else:
            agent = random.choice(online)
            acts, probs = agent.get_action_probabilities()
            action = random.choices(acts, weights=probs, k=1)[0]

        event_data = agent.generate_event(action)
        producer.send(TOPIC, key=agent.player_id, value=event_data)
        
        count += 1
        if count % 100 == 0:
            elapsed = time.time() - start_time
            print(f">>> Sent {count} events. Avg Speed: {count/elapsed:.1f} events/s", flush=True)

        process_time = time.time() - loop_start
        if process_time < SLEEP_TIME:
            time.sleep(SLEEP_TIME - process_time)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Producer Crashed: {e}")
        while True: time.sleep(60)