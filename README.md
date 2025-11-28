# 🎮 Real-time Game Analytics & Engagement Prediction System

Dự án Big Data mô phỏng hệ thống phân tích hành vi người chơi game theo thời gian thực (Real-time). Hệ thống sử dụng **Kafka** để truyền tải dữ liệu, **Spark Streaming** để xử lý và dự đoán độ tương tác (Engagement Level) bằng mô hình Machine Learning, và **Redis + WebUI** để hiển thị Dashboard trực quan.

## 🚀 Kiến trúc Hệ thống

Luồng dữ liệu (Data Pipeline) hoạt động như sau:

1.  **Data Source (Producer):**
    * Sử dụng script Python để mô phỏng hành vi người chơi dựa trên tập dữ liệu gốc `online_gaming_behavior_dataset.csv`.
    * Áp dụng kỹ thuật **Stratified Sampling** để chọn mẫu người chơi đại diện (High/Medium/Low Engagement).
    * Sinh các sự kiện (Events): `login`, `logout`, `purchase`, `level_up`, `achievement_unlocked` và gửi tới Kafka.

2.  **Message Broker (Kafka):**
    * Tiếp nhận và đệm (buffer) các stream sự kiện từ Producer.
    * Đảm bảo tính tuần tự và tin cậy của dữ liệu.

3.  **Stream Processing (Spark Streaming):**
    * Đọc dữ liệu từ Kafka.
    * **Phase 1:** Cập nhật ngay lập tức các chỉ số thô (Real-time Counters) vào Redis.
    * **Phase 2:** Tính toán các chỉ số phái sinh (Avg Session Duration, Total Play Time...) và sử dụng mô hình **Spark ML Pipeline** (`cv_pipeline_model`) đã được huấn luyện trước để dự đoán **Engagement Level** (High/Medium/Low) của người chơi.

4.  **Storage (Redis):**
    * Lưu trữ trạng thái người chơi (State Store) và các kết quả dự đoán với tốc độ truy xuất cực nhanh.

5.  **Visualization (WebUI):**
    * Backend: Python Flask API đọc dữ liệu từ Redis.
    * Frontend: Dashboard HTML/JS hiển thị các chỉ số thời gian thực (Online users, Total spent, Live events...).

## 🛠️ Công nghệ sử dụng

* **Ngôn ngữ:** Python (Pyspark, Flask, Kafka-python).
* **Big Data Core:** Apache Spark (Streaming & MLlib), Apache Kafka, Zookeeper.
* **Database:** Redis (In-memory Key-Value Store).
* **Containerization:** Docker & Docker Compose.
* **Frontend:** HTML5, CSS3, JavaScript.

## 📂 Cấu trúc Dự án

```
├── docker-compose.yml # File cấu hình toàn bộ hệ thống Docker 
├── data/ # Chứa dataset gốc (.csv) 
├── cv_pipeline_model/ # Mô hình Spark ML đã huấn luyện (PipelineModel) 
├── producer/ # Service sinh dữ liệu giả lập 
│ ├── producer.py # Script chính gửi event vào Kafka 
│ └── Dockerfile 
├── spark/ # Service xử lý dữ liệu 
│ ├── app/spark_streaming.py # Logic chính của Spark Streaming 
│ └── Dockerfile 
├── webui/ # Giao diện Dashboard 
│ ├── backend/ # Flask API 
│ ├── frontend/ # Giao diện HTML/CSS/JS 
│ └── Dockerfile 
└── notebook/ # Các Jupyter Notebook dùng để train model và phân tích
```

## ⚙️ Hướng dẫn Cài đặt & Chạy

### Yêu cầu tiên quyết
* Docker Desktop đã được cài đặt và đang chạy.

### Bước 1: Khởi động hệ thống
Mở terminal tại thư mục gốc của dự án và chạy lệnh:

```bash
docker-compose up -d --build
```

Lệnh này sẽ build các image (Producer, Spark, WebUI) và khởi động các container (Zookeeper, Kafka, Redis).

### Bước 2: Truy cập Dashboard
Sau khi các container đã khởi động thành công (đợi khoảng 30s - 1 phút để Spark và Kafka ổn định), mở trình duyệt và truy cập:

* URL: http://localhost:5000

### Bước 3: Reset dữ liệu (Quan trọng)
Nếu bạn muốn chạy lại từ đầu hoặc thay đổi code Producer/Spark, hãy xóa các volume cũ để tránh xung đột dữ liệu trong Redis/Kafka:

```bash
# Dừng container và xóa volumes
docker-compose down -v

# Khởi động lại
docker-compose up -d --build
```

## 📊 Các tính năng nổi bật trên Dashboard
1. **Global Metrics:** Hiển thị tổng số sự kiện, số người đang online, tổng doanh thu in-game.

2. **Live Event Feed:** Log chi tiết các hành động (Login, Purchase...) đang diễn ra.

3. **Real-time Player Inspection:**
    * Chọn một người chơi để xem hồ sơ chi tiết.
    * Xem AI dự đoán mức độ nghiện game (Engagement Level) cập nhật từng giây.
    * Theo dõi Total Time Played, Session Duration, và Achievements.

## 📝 Ghi chú phát triển
Mô hình AI được huấn luyện trong `notebook/model4.ipynb` và được export ra thư mục `cv_pipeline_model`.

Để thay đổi tốc độ sinh dữ liệu, chỉnh sửa biến `TARGET_RPS` trong `producer/producer.py`.

---
## 👥 Nhóm Tác Giả

| STT | Họ và Tên | Mã Sinh Viên |
| :---: | :--- | :--- |
| 1 | **Phạm Hải Tiến** | 23020425 |
| 2 | **Mai Phan Anh Tùng** | 23020433 |
| 3 | **Phạm Quốc Hùng** | 23020373 |