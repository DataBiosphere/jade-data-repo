import argparse
import os
import subprocess
import time
import csv
import datetime

from src.parse_gcp_log import parse_gcp_log
from src.populate_user_details_from_data_warehouse import populate_user_details

from data_repo_client import (
    Configuration,
    ApiClient,
    ProfilesApi,
    DatasetsApi,
    SnapshotsApi,
    JobsApi,
    SnapshotAccessRequestApi,
)


class Clients:
    def __init__(self, host):
        config = Configuration()
        config.host = host
        token_output = subprocess.run(
            ["gcloud", "auth", "print-access-token"], capture_output=True
        )
        config.access_token = token_output.stdout.decode("UTF-8").strip()
        self.api_client = ApiClient(configuration=config)

        self.profiles_api = ProfilesApi(api_client=self.api_client)
        self.datasets_api = DatasetsApi(api_client=self.api_client)
        self.snapshots_api = SnapshotsApi(api_client=self.api_client)
        self.jobs_api = JobsApi(api_client=self.api_client)
        self.snapshot_request_api = SnapshotAccessRequestApi(api_client=self.api_client)


def wait_for_job(clients, job_model):
    result = clients.jobs_api.retrieve_job(job_model.id)
    while True:
        if result is None or result.job_status == "running":
            time.sleep(10)
            print(f"Waiting for job {job_model.id} to finish")
            result = clients.jobs_api.retrieve_job(job_model.id)
        elif result.job_status == "failed":
            result = clients.jobs_api.retrieve_job_result(job_model.id)
            raise Exception(
                f"Could not complete job with id {job_model.id}, got result {result}"
            )
        elif result.job_status == "succeeded":
            print(f"Job succeeded {job_model.id}: {job_model.description}")
            result = clients.jobs_api.retrieve_job_result(job_model.id)
            return result
        else:
            raise "Unrecognized job state %s" % result.job_status


def wait_for_jobs(clients, jobs):
    for job in jobs:
        wait_for_job(clients, job)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=False,
        default="https://data.terra.bio",
        help="The data repo root URL to point to. Defaults to production TDR (https://data.terra.bio).`",
    )
    parser.add_argument(
        "--raw_logs_location",
        required=True,
        help="A file pointer to the raw logs to convert. This is required flag. Available Options: "
             + ", ".join(os.listdir("./rawLogs/")),
    )
    args = parser.parse_args()
    clients = Clients(args.host)
    raw_logs_location = args.raw_logs_location

    emails = set()
    dataset_ids = set()
    snapshot_ids = set()

    populated_new_logs = parse_gcp_log(raw_logs_location, emails, dataset_ids, snapshot_ids)
    # Get user details from Thurloe/data warehouse
    populate_user_details(populated_new_logs, emails)

    # For Snapshots, get auth domains and source datasets from TDR
    user_permission_group = ""
    # Can access via the snapshot policies endpoint
    # Can enumerate snapshots and filter by snapshotIds to get source dataset

    # For Datasets, get PHSIds from TDR
    associated_study = ""
    # Will need to use the enumerate datasets endpoint and filter by datasetIds


    # Pull ERA commmons id
    eRA_commons_id = ""
    # Not yet relevant


    # Write to desired format
    outputs = []
    outputs.append(["_time", "src_ip", "dest_ip", "dest_port", "user_name", "user_id", "user_id_provider", "session_id", "method", "url", "app", "http_user_agent", "status", "http_content_type", "bytes", "duration", "nih_ico", "cadr_name", "user_country_name", "user_org", "user_email", "associated_study", "eRA_commons_id", "user_permission_group", "event_type", "dataset_id", "snapshot_id"])
    for log in populated_new_logs:
        outputs.append([log._time, log.src_ip, log.dest_ip, log.dest_port, log.user_name, log.user_id, log.user_id_provider, log.session_id, log.method, log.url, log.app, log.http_user_agent, log.status, log.http_content_type, log.bytes, log.duration, log.nih_ico, log.cadr_name, log.user_country_name, log.user_org, log.user_email, log.associated_study, log.eRA_commons_id, log.user_permission_group, log.event_type, log.dataset_id, log.snapshot_id])

    output_filename = f"output/{os.path.basename(args.raw_logs_location).split('.')[0]}_{datetime.datetime.now()}.csv"

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in outputs:
            writer.writerow(row)


    print(f"\n\n\nDONE. Logs written to {output_filename}")

if __name__ == "__main__":
    main()
