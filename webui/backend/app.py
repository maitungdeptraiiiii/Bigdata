from flask import Flask, jsonify, render_template, request, redirect, url_for
import redis
import json
import os

# Cấu hình Flask trỏ template về thư mục frontend
app = Flask(__name__, template_folder='../frontend', static_folder='../frontend')

# Config Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# --- ROUTE GIAO DIỆN ---

@app.route('/')
def index():
    """Trang chủ Dashboard"""
    return render_template('index.html')

@app.route('/player/<player_id>')
def player_detail(player_id):
    """Trang chi tiết Player"""
    # Kiểm tra xem player có tồn tại trong Redis không
    if not redis_client.exists(f"player:{player_id}"):
        return render_template('index.html', error="Player not found")
    return render_template('player.html', player_id=player_id)

# --- API DATA ---

@app.route('/api/stats')
def get_stats():
    """Lấy thống kê tổng quan"""
    metrics = redis_client.hgetall("live_metrics")
    
    # Đếm số lượng online thủ công (scan keys) hoặc nếu spark đã tính thì lấy ra
    # Ở đây ta loop qua tập player mẫu để đếm cho nhanh (demo)
    all_players = redis_client.smembers("all_player_ids")
    online_count = 0
    for pid in all_players:
        status = redis_client.hget(f"player:{pid}", "status")
        if status == 'online':
            online_count += 1
            
    return jsonify({
        "total_events": metrics.get("total_events", 0),
        "online_users": online_count,
        "total_users": len(all_players)
    })

@app.route('/api/events')
def get_events():
    """Lấy log sự kiện gần đây"""
    raw_events = redis_client.lrange("recent_events_list", 0, 19)
    events = [json.loads(e) for e in raw_events]
    return jsonify(events)

@app.route('/api/players_list')
def get_players_list():
    """Lấy danh sách ID và Tên (nếu có) để điền vào thanh search"""
    # Chỉ lấy tối đa 100 người để dropdown không bị lag
    all_ids = list(redis_client.smembers("all_player_ids"))[:100]
    players = []
    for pid in all_ids:
        # Lấy thêm status để hiển thị màu trong dropdown
        status = redis_client.hget(f"player:{pid}", "status") or "offline"
        players.append({"id": pid, "status": status})
    
    # Sort online lên đầu
    players.sort(key=lambda x: x['status'] == 'offline')
    return jsonify(players)

@app.route('/api/player/<pid>')
def get_player_detail(pid):
    """Lấy chi tiết dữ liệu của 1 player"""
    data = redis_client.hgetall(f"player:{pid}")
    if not data:
        return jsonify({"error": "Not found"}), 404
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)