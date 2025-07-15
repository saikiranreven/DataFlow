from google.cloud import pubsub_v1
import json, time, random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_message():
    return {
        "user_id": f"user_{random.randint(1,100)}",
        "action": random.choice(["click", "purchase", "view", "login", "logout"]),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def publish_messages(project_id, topic_name):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    try:
        while True:
            data = generate_message()
            future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
            message_id = future.result()
            logger.info(f"Published message {message_id}: {data}")
            time.sleep(random.uniform(0.5, 2.5))
    except KeyboardInterrupt:
        logger.info("Publisher stopped")

if __name__ == "__main__":
    import os
    project_id = os.getenv("PROJECT_ID", "bct-project-465419")
    topic_name = os.getenv("TOPIC_NAME", "stream-topic")
    publish_messages(project_id, topic_name)