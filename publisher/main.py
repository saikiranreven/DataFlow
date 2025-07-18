from fastapi import FastAPI
import uvicorn
from google.cloud import pubsub_v1
import json, time, random, threading, os

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def generate_message(topic_id):
    """Generate different message formats for each topic"""
    if topic_id == "stream-topic-1":
        return {
            "user_id": f"user_{random.randint(1,100)}",
            "action": random.choice(["click", "view", "purchase"]),
            "source": "topic1",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
    else:
        return {
            "event_id": f"evt_{random.randint(1000,9999)}",
            "type": random.choice(["login", "error", "timeout"]),
            "source": "topic2",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }

def publish_messages():
    publisher = pubsub_v1.PublisherClient()
    # Get topics from environment variables
    topic1_path = publisher.topic_path(os.getenv("PROJECT_ID"), "stream-topic-1")
    topic2_path = publisher.topic_path(os.getenv("PROJECT_ID"), "stream-topic-2")
    
    while True:
        try:
            # Publish to Topic 1
            data1 = generate_message("stream-topic-1")
            publisher.publish(topic1_path, json.dumps(data1).encode("utf-8"))
            
            # Publish to Topic 2
            data2 = generate_message("stream-topic-2")
            publisher.publish(topic2_path, json.dumps(data2).encode("utf-8"))
            
            time.sleep(random.uniform(0.5, 2.5))
        except Exception as e:
            print(f"Publish error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # Start publisher thread
    threading.Thread(target=publish_messages, daemon=True).start()
    
    # Start HTTP server
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))