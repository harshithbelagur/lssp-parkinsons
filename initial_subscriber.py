from pyspark.sql import SparkSession
from google.cloud import pubsub_v1
import threading
import argparse
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_timestamp, window, avg
from joblib import load
from google.cloud import storage
from io import BytesIO

project_id = "stream-processing-414521"
topic_id = "model-inter-output"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

storage_client = storage.Client()
bucket = storage_client.get_bucket('lssp-bucket')

# Define the schema of your data
schema = StructType([
    StructField("Timestamp", StringType(), False),
    StructField("Axis1", StringType(), False),
    StructField("Axis2", StringType(), False),
    StructField("Axis3", StringType(), False),
    StructField("Gyro1", StringType(), False),
    StructField("Gyro2", StringType(), False),
    StructField("Gyro3", StringType(), False)
])

def create_windows(spark, messages):
    df = spark.createDataFrame(messages, schema)
    df = df.orderBy(col("Timestamp").asc())

    # List of column names to convert
    columns_to_convert = ["Timestamp","Axis1", "Axis2", "Axis3", "Gyro1", "Gyro2", "Gyro3"]

    # Convert each column to DoubleType
    for column_name in columns_to_convert:
        df = df.withColumn(column_name, col(column_name).cast("double"))

    df = df.withColumn("Timestamp", to_timestamp(col("Timestamp")).cast("Timestamp"))

    windowed_df = df.groupBy(
    window(col("Timestamp"), "5 seconds")
    ).agg(
        avg("Axis1").alias("Axis1"),
        avg("Axis2").alias("Axis2"),
        avg("Axis3").alias("Axis3"),
        avg("Gyro1").alias("Gyro1"),
        avg("Gyro2").alias("Gyro2"),
        avg("Gyro3").alias("Gyro3")
    ).select(
        "window.start", "window.end",
        "Axis1", "Axis2", "Axis3", "Gyro1", "Gyro2","Gyro3"
    )

    return windowed_df

def run_model(windowed_df, sensor_name):
    feature_columns = ['Axis1','Axis2','Axis3','Gyro1','Gyro2','Gyro3']
    selected_df = windowed_df.select(feature_columns)
    pandas_df = selected_df.toPandas()
    numpy_array = pandas_df.values
    print(numpy_array)

    predictions = []

    # Load ML Model using joblib
    first_five_blob = bucket.blob(sensor_name + '_first_five.joblib')
    model_file = BytesIO()
    first_five_blob.download_to_file(model_file)
    model = load(model_file)
    predictions.extend(model.predict([numpy_array[0]]))

    rest_blob = bucket.blob(sensor_name + '_rest.joblib')
    model_file = BytesIO()
    rest_blob.download_to_file(model_file)
    model = load(model_file)
    predictions.extend(model.predict([numpy_array[1]]))

    print(predictions)

    array_str = ' '.join([str(pred) for pred in predictions])
    return array_str

def publish_message(sensor_name, data):
    data = sensor_name + ' ' + data
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

def consume_messages(spark, subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path("stream-processing-414521", subscription_id)
    
    messages = []   
    def callback(message):
        print(f"Received message: {message.data.decode('utf-8')}")
        messages.append(message.data.decode('utf-8').split(","))
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on {}".format(subscription_path))
    
    # Schedule the subscriber to stop after 300 seconds
    timer = threading.Timer(300, streaming_pull_future.cancel)
    timer.start()
    
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Streaming stopped with exception: {e}")
    finally:
        timer.cancel()  # Ensure the timer is cleaned up if exiting early
    
    return messages

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SimpleMessageConsumer").getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument("--sensor_name", required=True)
    args = parser.parse_args()
    subscription_id = args.sensor_name + "-subscription"
    messages = consume_messages(spark, subscription_id)
    windowed_df = create_windows(spark, messages)
    array_str = run_model(windowed_df, args.sensor_name)
    publish_message(args.sensor_name, array_str)
    spark.stop()