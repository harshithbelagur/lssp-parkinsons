# import functions_frame÷work
from google.cloud import datastore
from google.cloud import dataproc_v1
import time
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

datastore_client = datastore.Client('stream-processing-414521')

def start_job(project_id, region, cluster, sensor_name):
    """
    Checks for active Dataproc jobs in the specified project and region.
    
    Args:
        project_id (str): The ID of the Google Cloud project.
        region (str): The region in which to check for Dataproc jobs.
    """
    global flag
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
        global flag
        if (job.pyspark_job.main_python_file_uri == "gs://lssp-bucket/final_evaluator.py") and (job.status.state in do_not_start_states):
            print(job)
    
    # if not flag:
    #     job_client = dataproc_v1.JobControllerClient(
    #         client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    #     )

    #     job = {
    #         'placement': {
    #             'cluster_name': cluster
    #         },
    #         'pyspark_job': {
    #             'main_python_file_uri': 'gs://lssp-bucket/final_evaluator.py',
    #         }
    #     }

    #     result = job_client.submit_job(project_id=project_id, region=region, job=job)
    #     print("Job submitted.")
    #     return result

    # if jobs:
    #     print(f"Found {len(jobs)} active Dataproc job(s) in project {project_id} and region {region}.")
    # else:
    #     print(f"No active Dataproc jobs found in project {project_id} and region {region}.")

# Triggered from a message on a Cloud Pub/Sub topic
# @functions_framework.cloud_event

project_id = 'stream-processing-414521'
region = 'us-east4'
cluster = 'my-cluster'
sensor_name = 'final-model'
start_job(project_id, region, cluster, sensor_name)