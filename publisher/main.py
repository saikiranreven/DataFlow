# publisher/main.py
from fastapi import FastAPI
import uvicorn
from google.cloud import pubsub_v1
import json, time, random, threading, os

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def generate_message():
    return {
        "user_id": f"user_{random.randint(1,100)}",
        "action": random.choice(["click", "purchase", "view", "login", "logout"]),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def publish_messages():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_NAME"))
    
    while True:
        data = generate_message()
        future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        message_id = future.result()
        print(f"Published message {message_id}: {data}")
        time.sleep(random.uniform(0.5, 2.5))

if __name__ == "__main__":
    # Start publisher thread
    threading.Thread(target=publish_messages, daemon=True).start()
    
    # Start HTTP server
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))