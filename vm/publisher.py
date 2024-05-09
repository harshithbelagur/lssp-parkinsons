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
    # print(f"Published message ID: {future.result()}")

def read_and_publish(file_path, topic_id):
    print(file_path)
    print(topic_id)
    with open(file_path, "r") as file:
        for line in file:
                publish_message(line.strip(), topic_id)

if __name__ == "__main__":
    file_path = sys.argv[1]
    if os.path.isfile(file_path):
        match = re.match(r"^001_(.+?)(\.\w+)?$", os.path.basename(file_path))
        if match:
            extracted_text = match.group(1)
            read_and_publish(file_path, extracted_text.lower())
    else:
        print("Invalid file path.")