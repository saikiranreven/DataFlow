# main.py
from flask import Flask
import threading
import time
import json
import random
from google.cloud import pubsub_v1
from datetime import datetime, timezone

app = Flask(__name__)
running = False  # prevent duplicate threads

def publish_loop():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("bct-project-465419", "stream-topic")
    actions = ["click", "purchase", "view"]

    while True:
        message = {
            "user_id": f"user_{random.randint(1, 100)}",
            "action": random.choice(actions),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        print(f"Published: {message}")
        time.sleep(40)

@app.route("/")
def health():
    return "OK"

@app.route("/start")
def start():
    global running
    if not running:
        thread = threading.Thread(target=publish_loop, daemon=True)
        thread.start()
        running = True
        return "Publisher started"
    return "Already running"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)