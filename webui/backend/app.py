from flask import Flask, Response, render_template, send_from_directory
import time
import json
import redis
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_KEY = os.environ.get("REDIS_KEY", "live_metrics")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = Flask(__name__, static_folder='../frontend', template_folder='../frontend')

@app.route('/')
def index():
    return render_template('index.html')


def event_stream(poll_interval=1.0):
    last = None
    while True:
        try:
            raw = r.get(REDIS_KEY)
            if raw:
                if raw != last:
                    last = raw
                    yield f"data: {raw}\n\n"
            time.sleep(poll_interval)
        except GeneratorExit:
            break
        except Exception:
            time.sleep(poll_interval)


@app.route('/metrics/stream')
def stream_metrics():
    headers = {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    return Response(event_stream(), headers=headers)


@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(app.static_folder, filename)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
