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

# Số lượng mẫu cho mỗi nhóm
SAMPLES_PER_GROUP = 10 

sys.stdout.reconfigure(line_buffering=True)

class PlayerAgent:
    def __init__(self, profile):
        self.profile = profile
        self.player_id = profile['PlayerID']
        self.is_online = False
        self.current_level = int(profile.get('PlayerLevel', 1))
        
        # Lấy chỉ số gốc để mô phỏng hành vi
        # Nếu không parse được thì dùng giá trị mặc định
        try:
            self.avg_duration = float(profile.get('AvgSessionDurationMinutes', 30))
            self.sessions_per_week = float(profile.get('SessionsPerWeek', 5))
            self.engagement_group = profile.get('EngagementLevel', 'Medium')
        except:
            self.avg_duration = 30.0
            self.sessions_per_week = 5.0
            self.engagement_group = 'Medium'

        # Tính toán trọng số login dựa trên SessionsPerWeek
        # Người chơi nhiều -> Trọng số cao -> Dễ được chọn để login
        self.login_weight = self.sessions_per_week

    def get_action_probabilities(self):
        """
        Trả về xác suất hành động khi đang Online.
        Người High Engagement sẽ dễ mua đồ và cày cuốc hơn.
        """
        # Default weights: [game_play, purchase, level_up, achievement, logout]
        if self.engagement_group == 'High':
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.6, 0.15, 0.05, 0.1, 0.1]
        elif self.engagement_group == 'Medium':
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.6, 0.05, 0.05, 0.1, 0.2]
        else: # Low
            return ["game_play", "purchase", "level_up", "achievement_unlocked", "logout"], [0.5, 0.01, 0.02, 0.02, 0.45]

    def generate_event(self, event_type):
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        base_event = {
            "event_id": f"evt_{int(time.time()*1000)}_{random.randint(100,999)}",
            "player_id": self.player_id,
            "event_type": event_type,
            "timestamp": timestamp
        }

        if event_type == "login":
            self.is_online = True
            
        elif event_type == "logout":
            self.is_online = False
            # Mô phỏng thời gian chơi dựa trên AvgSessionDurationMinutes của chính họ
            # Dùng phân phối chuẩn (Gauss) để tạo biến động tự nhiên
            # Độ lệch chuẩn (sigma) = avg / 4
            duration = random.normalvariate(self.avg_duration, self.avg_duration/4)
            # Kẹp giá trị để không bị âm hoặc quá vô lý
            duration = max(1.0, min(duration, 300.0)) 
            base_event["session_duration"] = round(duration, 2)

        elif event_type == "purchase":
            # Người High thường chi nhiều tiền hơn
            if self.engagement_group == 'High':
                amt = random.uniform(4.99, 99.99)
            else:
                amt = random.uniform(0.99, 9.99)
                
            base_event["item_id"] = f"item_{random.randint(1, 100)}"
            base_event["amount"] = round(amt, 2)

        elif event_type == "level_up":
            self.current_level += 1
            base_event["new_level"] = self.current_level

        return base_event

def load_stratified_players():
    """Chọn 10 High, 10 Med, 10 Low từ CSV"""
    print(f"Loading players from {CSV_FILE}...", flush=True)
    if not os.path.exists(CSV_FILE):
        print("CSV not found!")
        return []

    high_group, med_group, low_group = [], [], []
    
    with open(CSV_FILE, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            level = row.get('EngagementLevel', 'Medium')
            if level == 'High': high_group.append(row)
            elif level == 'Medium': med_group.append(row)
            else: low_group.append(row)

    print(f"Found in CSV: High={len(high_group)}, Med={len(med_group)}, Low={len(low_group)}", flush=True)

    # Helper safe sample
    def safe_sample(population, k):
        return random.sample(population, min(len(population), k))

    selected = (
        safe_sample(high_group, SAMPLES_PER_GROUP) +
        safe_sample(med_group, SAMPLES_PER_GROUP) +
        safe_sample(low_group, SAMPLES_PER_GROUP)
    )
    
    random.shuffle(selected) # Trộn đều để khi gửi register không bị gom cục
    return selected

def create_producer():
    # (Giữ nguyên logic kết nối an toàn)
    print(f"Connecting to Kafka at {BOOTSTRAP}...", flush=True)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None
            )
            print(">>> Kafka Connected!", flush=True)
            return producer
        except NoBrokersAvailable:
            print(">>> Kafka not ready. Retrying...", flush=True)
            time.sleep(3)

def main():
    producer = create_producer()
    print(">>> Waiting 15s for Spark...", flush=True)
    time.sleep(15)

    # 1. Load & Init Agents
    profiles = load_stratified_players()
    if not profiles:
        print("No players loaded. Exiting.")
        return

    agents = [PlayerAgent(p) for p in profiles]
    agents_dict = {a.player_id: a for a in agents} # Dễ truy xuất
    print(f">>> Initialized {len(agents)} agents (Stratified).", flush=True)

    # 2. Register
    print(">>> Sending Registers...", flush=True)
    for ag in agents:
        # Gửi kèm EngagementLevel gốc để Spark biết Ground Truth (nếu cần)
        reg_event = {
            "event_type": "register",
            "player_id": ag.player_id,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "Age": int(ag.profile.get('Age', 0)),
            "Gender": ag.profile.get('Gender', 'Unknown'),
            "Location": ag.profile.get('Location', 'Unknown'),
            "GameGenre": ag.profile.get('GameGenre', 'Unknown'),
            "GameDifficulty": ag.profile.get('GameDifficulty', 'Medium'),
            "EngagementLevel": ag.engagement_group, # Gửi cái này để debug xem AI đoán đúng ko
            "status": "offline"
        }
        producer.send(TOPIC, key=ag.player_id, value=reg_event)
        time.sleep(0.02)
    
    producer.flush()
    time.sleep(5)

    # 3. Simulation Loop (Smart Selection)
    print(">>> Starting Smart Simulation...", flush=True)
    while True:
        # Bước A: Chọn người để hành động
        # Logic: Ta muốn người Online thì hoạt động, người Offline thì Login
        # Nhưng phải ưu tiên người có session/week cao login thường xuyên hơn
        
        # Tách 2 nhóm
        online_agents = [a for a in agents if a.is_online]
        offline_agents = [a for a in agents if not a.is_online]

        # Quyết định xem lượt này là Login hay Action của người đang Online
        # Tỷ lệ: 30% chọn người mới Login, 70% chọn người đang Online hành động
        # (Nếu không ai online thì bắt buộc login)
        
        mode = "login"
        if online_agents and random.random() > 0.3:
            mode = "action"
        
        if not offline_agents: mode = "action"
        if not online_agents: mode = "login"

        selected_agent = None
        action = None

        if mode == "login":
            # Chọn người login dựa trên trọng số (SessionsPerWeek)
            # Người chơi chăm chỉ -> Trọng số cao -> Dễ được pick
            weights = [a.login_weight for a in offline_agents]
            selected_agent = random.choices(offline_agents, weights=weights, k=1)[0]
            action = "login"
        else:
            # Chọn ngẫu nhiên 1 người đang online để làm gì đó
            selected_agent = random.choice(online_agents)
            # Lấy hành động dựa trên xác suất của nhóm Engagement (High/Med/Low)
            acts, probs = selected_agent.get_action_probabilities()
            action = random.choices(acts, weights=probs, k=1)[0]

        # Bước B: Thực hiện
        event_data = selected_agent.generate_event(action)
        producer.send(TOPIC, key=selected_agent.player_id, value=event_data)
        
        # Log đẹp để dễ nhìn
        grp = selected_agent.engagement_group
        print(f"User {selected_agent.player_id} ({grp}) -> {action}", flush=True)

        # Tốc độ giả lập
        time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Producer Error: {e}")
        while True: time.sleep(60)