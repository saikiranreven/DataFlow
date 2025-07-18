from fastapi import FastAPI
import uvicorn
from google.cloud import pubsub_v1
import json
import time
import random
import threading
import os

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def generate_message(topic_id):
    """Generate different message formats for each topic with enhanced logging"""
    if topic_id == "stream-topic-1":
        message = {
            "user_id": f"user_{random.randint(1,100)}",
            "action": random.choice(["click", "view", "purchase"]),
            "source": "topic1",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
    else:
        message = {
            "event_id": f"evt_{random.randint(1000,9999)}",
            "type": random.choice(["login", "error", "timeout"]),
            "source": "topic2",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
    print(f"📝 Generated message for {topic_id}: {json.dumps(message)}")
    return message

def publish_messages():
    """Publish messages to both topics with detailed success/error logging"""
    publisher = pubsub_v1.PublisherClient()
    
    # Get configuration from environment
    project_id = os.getenv("PROJECT_ID")
    topic1_name = "stream-topic-1"  # Must match Terraform
    topic2_name = "stream-topic-2"  # Must match Terraform
    
    if not project_id:
        print("❌ Critical Error: PROJECT_ID environment variable not set!")
        return

    topic1_path = publisher.topic_path(project_id, topic1_name)
    topic2_path = publisher.topic_path(project_id, topic2_name)
    
    print(f"🚀 Starting publisher for:\n- {topic1_path}\n- {topic2_path}")

    while True:
        try:
            # Publish to Topic 1 with confirmation
            data1 = generate_message("stream-topic-1")
            future1 = publisher.publish(topic1_path, json.dumps(data1).encode("utf-8"))
            message_id1 = future1.result()  # Wait for publish to complete
            print(f"✅ [Topic1] Published {message_id1}: {json.dumps(data1)}")

            # Publish to Topic 2 with confirmation
            data2 = generate_message("stream-topic-2")
            future2 = publisher.publish(topic2_path, json.dumps(data2).encode("utf-8"))
            message_id2 = future2.result()  # Wait for publish to complete
            print(f"✅ [Topic2] Published {message_id2}: {json.dumps(data2)}")

        except Exception as e:
            print(f"❌ Publish failed: {str(e)}")
            time.sleep(5)  # Longer delay on errors
        else:
            time.sleep(random.uniform(0.5, 2.5))  # Normal delay

if __name__ == "__main__":
    # Validate environment before starting
    required_vars = ["PROJECT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    else:
        print("🏁 Starting publisher service...")
        threading.Thread(target=publish_messages, daemon=True).start()
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))