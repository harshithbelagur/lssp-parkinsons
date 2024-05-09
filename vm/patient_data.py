from google.cloud import pubsub_v1
import os
import re
import sys

# Environment variables for credentials and project
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
project_id = "stream-processing-414521"

def publish_message(data, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    data = 'Patient Data: 56,56,173,78,1,1,1'
    publish_message(data, 'patient_data')
