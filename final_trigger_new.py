import functions_framework
from google.cloud import datastore
from google.cloud import dataproc_v1
import time

datastore_client = datastore.Client('stream-processing-414521')

def acquire_lock(sensor_name):
    lock_key = datastore_client.key(sensor_name, 'lock')
    with datastore_client.transaction():
        lock_entity = datastore_client.get(lock_key)
        if not lock_entity or not lock_entity['locked']:
            lock_entity = datastore.Entity(key=lock_key, exclude_from_indexes=['locked'])
            lock_entity['locked'] = True
            datastore_client.put(lock_entity)
            return True
    return False

def release_lock(sensor_name):
    lock_key = datastore_client.key(sensor_name, 'lock')
    with datastore_client.transaction():
        lock_entity = datastore_client.get(lock_key)
        if lock_entity:
            lock_entity['locked'] = False
            datastore_client.put(lock_entity)

def start_dataproc_job(project_id, region, cluster, sensor_name):
    if acquire_lock(sensor_name):
        try:
            # Start the Dataproc job
            start_job(project_id, region, cluster, sensor_name)
            time.sleep(3)
        finally:
            release_lock(sensor_name)
    else:
        print(f"Failed to acquire lock for sensor: {sensor_name}")

def start_job(project_id, region, cluster, sensor_name):
    """
    Checks for active Dataproc jobs in the specified project and region.
    
    Args:
        project_id (str): The ID of the Google Cloud project.
        region (str): The region in which to check for Dataproc jobs.
    """
    flag = False
    client = dataproc_v1.JobControllerClient(
      client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # List active Dataproc jobs
    jobs = client.list_jobs(
        request={
            "project_id": project_id,
            "region": region,
        }
    )

    # # job.status.state = 5 => DONE
    # # job.status.state = 2 => RUNNING
    # # job.status.state = 6 => ERROR
    # # check others at https://googleapis.github.io/googleapis/java/all/latest/apidocs/com/google/cloud/dataproc/v1/JobStatus.State.html

    do_not_start_states = [1, 2, 8]

    print('Entered here though')

    for job in jobs:
        if (job.pyspark_job.main_python_file_uri == "gs://lssp-bucket/final_evaluator.py") and (job.status.state in do_not_start_states):
            flag = True

    if flag == False:
        job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        job = {
            'placement': {
                'cluster_name': cluster
            },
            'pyspark_job': {
                'main_python_file_uri': 'gs://lssp-bucket/final_evaluator.py',
            }
        }

        result = job_client.submit_job(project_id=project_id, region=region, job=job)
        print("Job submitted.")
        return result

    # if jobs:
    #     print(f"Found {len(jobs)} active Dataproc job(s) in project {project_id} and region {region}.")
    # else:
    #     print(f"No active Dataproc jobs found in project {project_id} and region {region}.")

# Triggered from a message on a Cloud Pub/Sub topic
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    project_id = 'stream-processing-414521'
    region = 'us-east4'
    cluster = 'my-cluster'
    sensor_name = 'final-model'
    start_dataproc_job(project_id, region, cluster, sensor_name)