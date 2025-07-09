from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone  # Added timezone

project_id = "bct-project-465419"
topic_id = "stream-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
actions = ["click", "purchase", "view"]

print(f"Publishing to {topic_path} every 40 seconds...")

while True:
    message = {
        "user_id": f"user_{random.randint(1, 100)}",
        "action": random.choice(actions),
        "timestamp": datetime.now(timezone.utc).isoformat()  # Modern replacement
    }
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    print(f"Published: {message}")
    time.sleep(40)