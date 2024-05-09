from google.cloud import pubsub_v1
import os
import re

# Environment variables for credentials and project
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
project_id = "stream-processing-414521"

def publish_message(data, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

def read_and_publish(file_path, topic_id):
    counter = 0
    print(file_path)
    print(topic_id)
    with open(file_path, "r") as file:
        for line in file:
                publish_message(line.strip(), topic_id)

if __name__ == "__main__":

    directory_path = "pads-parkinsons-disease-smartwatch-dataset-1.0.0/movement/timeseries"
    files = os.listdir(directory_path)
    # matching_files = [file for file in files if file.startswith("001_")]
    pattern = r"^001_(.+?)(\.\w+)?$"
    extracted_texts = []
    for file in files:
        match = re.match(pattern, file)
        if match:
            extracted_text = match.group(1)
            extracted_texts.append(extracted_text)
    for text in extracted_texts:
        read_and_publish(directory_path+'/'+'001_'+text+'.txt', text.lower())
    # file_path = "pads-parkinsons-disease-smartwatch-dataset-1.0.0/movement/timeseries/001_CrossArms_LeftWrist.txt"
    # read_and_publish(file_path, topic_id)