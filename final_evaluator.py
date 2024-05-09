from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
from google.cloud import pubsub_v1
import re
from joblib import load
from google.cloud import storage
from io import BytesIO
import threading

project_id = "stream-processing-414521"
topic_id = "final-output"

storage_client = storage.Client()
bucket = storage_client.get_bucket('lssp-bucket')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

returned_predictions = []

patient_data = []

def count_zeros_ones():
    global returned_predictions
    lst = returned_predictions
    count_zeros = lst.count(0)
    count_ones = lst.count(1)
    
    if count_zeros > count_ones:
        return 0
    else:
        return 1
    
def get_patient_data(spark):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path("stream-processing-414521", 'patient_subscription')
    
    def callback(message):
        global patient_data
        if 'Patient' in message.data.decode('utf-8'):
            match = re.search(r'Patient Data:\s*(.*)', message.data.decode('utf-8'))
            if match:
                extracted_data = match.group(1)
                # print(extracted_data)
            else:
                print("No match found.")
            patient_data = [int(x) for x in extracted_data.split(',')]

        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on {}".format(subscription_path))
    
    # Schedule the subscriber to stop after 30 seconds
    timer = threading.Timer(30, streaming_pull_future.cancel)
    timer.start()
    
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Streaming stopped with exception: {e}")
    finally:
        timer.cancel()  # Ensure the timer is cleaned up if exiting early
    
def consume_messages(spark, subscription_id):
    global returned_predictions
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path("stream-processing-414521", subscription_id)

    messages = []
    
    def callback(message):
        nonlocal messages
        print(f"Received message: {message.data.decode('utf-8')}")
        messages.append(message.data.decode('utf-8'))

        # Regular expression to match numbers with spaces
        pattern = r'\b\d+\b'

        # Find all matches of the pattern in the input string
        numbers = re.findall(pattern, message.data.decode('utf-8'))

        if len(numbers)>0:
            # Convert the numbers to integers
            returned_predictions.extend([int(num) for num in numbers])

        message.ack()
        
        # Check if we have received 22 messages
        if len(messages) == 22:
            print("Received 22 messages, proceeding with processing...")
            subscriber.close()  # Optional: Close the subscriber after receiving 22 messages

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on {}".format(subscription_path))
    
    try:
        # Now we wait indefinitely as we handle stopping in the callback
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Subscription stopped due to: {e}")

def publish_message(data):
    data = str(data).encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

def process_messages(patient_data):
    # Process the list of 23 messages here
    print("Processing messages...")
    majority_voting_result = count_zeros_ones()
    first_five_blob = bucket.blob('final.joblib')
    model_file = BytesIO()
    first_five_blob.download_to_file(model_file)
    model = load(model_file)
    patient_data = np.array(patient_data).reshape(1, -1)
    model_output = model.predict(patient_data)
    print(majority_voting_result | model_output[0])
    publish_message(majority_voting_result | model_output[0])


def main():
    spark = SparkSession.builder.appName("Sklearn Prediction").getOrCreate()
    # Example DataFrame
    data = {'Feature1': [1, 2, 3, 4, 5], 'Feature2': [5, 4, 3, 2, 1], 'Target': [2, 2, 3, 3, 5]}
    pdf = pd.DataFrame(data)

    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)

    # Collect data back to a pandas DataFrame
    local_df = df.toPandas()

    # Fit model
    model = LinearRegression()
    model.fit(local_df[['Feature1', 'Feature2']], local_df['Target'])

    # Make predictions
    predictions = model.predict(local_df[['Feature1', 'Feature2']])
    print(predictions)
    subscription_id = 'model-subscription'
    consume_messages(spark, subscription_id)
    get_patient_data(spark)
    print(patient_data)
    process_messages(patient_data)

    spark.stop()

if __name__ == "__main__":
    main()