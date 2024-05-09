import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import time
from google.cloud import pubsub_v1

# Environment variables for credentials and project
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
project_id = "stream-processing-414521"

def get_final_output():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path("stream-processing-414521", 'final-output-subscription')
    
    messages = []

    def callback(message):
        nonlocal messages
        print(f"Received message: {message.data.decode('utf-8')}")
        messages.append(message.data.decode('utf-8'))
        if '1' in message.data.decode('utf-8'):
            print('''The patient has Parkinson's''')
        else:
            print('''The patient does not have Parkinson's''')
        print('Output is ', message)
        message.ack()
        
        # Check if we have received 1 messages
        if len(messages) == 1:
            subscriber.close()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on {}".format(subscription_path))
    
    try:
        # Now we wait indefinitely as we handle stopping in the callback
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Subscription stopped due to: {e}")

def publish_message(data, topic_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()

def run_publisher(file_path, topic_id):
    try:
        # print('Starting publishing at ', topic_id)
        with open(file_path, "r") as file:
            for line in file:
                publish_message(line.strip(), topic_id)
        print('Finished reading', file_path)
        return "Success"
    except Exception as e:
        return f"Error executing publisher: {str(e)}"

def run_publishers_for_folder(folder):
    # outputs = []
    # folder_name = os.path.basename(folder)
    with ThreadPoolExecutor(max_workers=2) as executor:
        files = [f for f in os.listdir(folder) if f.endswith(".txt")]
        # futures = []
        for file_name in files:
            match = re.match(r"^001_(.+?)(\.\w+)?$", file_name)
            if match:
                extracted_text = match.group(1).lower()
                file_path = os.path.join(folder, file_name)
                topic_id = f"{extracted_text}"
                print(f"Reading file: {file_path}")
                future = executor.submit(run_publisher, file_path, topic_id)
                # print('Finished reading')
                # futures.append((folder_name, file_name, future))
    #     for future in as_completed([f[2] for f in futures]):
    #         folder_name, file_name = None, None
    #         for f in futures:
    #             if f[2] == future:
    #                 folder_name, file_name = f[0], f[1]
    #                 break
    #         outputs.append((folder_name, file_name, future.result()))
    # return outputs

def list_folders():
    data_folder = "Data"
    # outputs = []
    folders = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder, f))]
    for folder in folders:
        run_publishers_for_folder(folder)

if __name__ == "__main__":
    start_time = time.time()
    list_folders()
    end_time = time.time()
    time_taken = end_time - start_time
    print('Took a total of ', str(time_taken), ' seconds')
    get_final_output()