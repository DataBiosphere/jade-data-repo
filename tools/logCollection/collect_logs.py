import argparse
import json
import os
import subprocess
import time
import uuid
import re
import csv
import datetime
from enum import Enum

from google.cloud import bigquery
from google.cloud import secretmanager

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



def get_raw_logs(filename):
    with open(os.path.join("rawLogs", filename)) as f:
        return json.load(f)


class EventType(Enum):
    DATA_ACCESS = 1
    DATA_UPLOAD = 2
    DATA_DELETION = 3
    OTHER = 4
def event_type_from_url(url, method):
    """
    Given a URL, determine the event type.
    Data Access:
    :check_green: /api/repository/v1/datasets/{id}/data/{table}
    :check_green: /api/repository/v1/datasets/{id}/data/{table}/statistics/{column}
    GET /api/repository/v1/datasets/{id}/files
    GET /api/repository/v1/datasets/{id}/files/{fileid}
    /api/repository/v1/datasets/{id}/filesystem/objects

    :check_green: /api/repository/v1/snapshots/{id}/data/{table}
    /api/repository/v1/snapshots/{id}/files
    /api/repository/v1/snapshots/{id}/export
    /api/repository/v1/snapshots/{id}/files/{fileid}
    /api/repository/v1/snapshots/{id}/filesystem/objects


    /ga4gh/drs/v1/objects/{object_id}
    /ga4gh/drs/v1/objects/{object_id}/access/{access_id}

    Data Uploads:
    /api/repository/v1/datasets/{id}/ingest
    POST /api/repository/v1/datasets/{id}/files
    /api/repository/v1/datasets/{id}/files/bulk
    /api/repository/v1/datasets/{id}/files/bulk/array

    Data Deletion:
    DELETE /api/repository/v1/snapshots/{id}
    POST /api/repository/v1/datasets/{id}/deletes
    DELETE /api/repository/v1/datasets/{id}
    DELETE /api/repository/v1/datasets/{id}/files/{fileid}
    """
    dataset_regex = re.compile(r'datasets/[0-9a-fA-F-]{36}$')
    snapshot_regex = re.compile(r'snapshots/[0-9a-fA-F-]{36}$')

    if "/data/" in url:
        return EventType.DATA_ACCESS
    elif "/ga4gh/drs/v1/objects/" in url and (method == "GET" or method == "POST"):
        return EventType.DATA_ACCESS
    elif "/files" in url:
        if "/bulk/array" in url:
            return EventType.DATA_UPLOAD
        # don't match on load tag endpoints
        regexp = re.compile(r'/files/bulk/[A-Za-z]+$')
        if regexp.search(url):
            return "Other"
        elif method == "DELETE":
            return EventType.DATA_DELETION
        elif method == "POST":
            return EventType.DATA_UPLOAD
        elif method == "GET":
            return EventType.DATA_ACCESS
    elif "filesystem/objects" in url:
        return EventType.DATA_ACCESS
    elif "/export" in url:
        return EventType.DATA_ACCESS
    elif "/ingest" in url:
        return EventType.DATA_UPLOAD
    elif dataset_regex.search(url) and method == "DELETE":
        return EventType.DATA_DELETION
    elif snapshot_regex.search(url) and method == "DELETE":
        return EventType.DATA_DELETION
    elif "/deletes" in url and method == "POST":
        return EventType.DATA_DELETION
    else:
        return EventType.OTHER

class UserDetails:
    def __init__(self, user_id=None, email=None, first_name=None, last_name=None, institute=None, country=None):
        self.user_id = user_id
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.institute = institute
        self.country = country

class Log:
    def __init__(self):
        self._time = None
        self.src_ip = None
        self.dest_ip = None
        self.dest_port = None
        self.user_name = None
        self.user_id = None
        self.user_id_provider = "Terra"
        self.session_id = None
        self.url = None
        self.app = "TDR"
        self.http_user_agent = None
        self.status = None
        self.http_content_type = None
        self.bytes = None
        self.duration = None
        self.nih_ico = None
        self.cadr_name = None
        self.user_country_name = None
        self.user_org = None
        self.user_email = None
        self.associated_study = None
        self.eRA_commons_id = "N/A"
        self.user_permission_group = None
        self.event_type = None
        self.dataset_id = None
        self.snapshot_id = None
        self.method = None

    def __str__(self):
        return f"{self._time}, {self.src_ip}, {self.dest_ip}, {self.dest_port}, {self.user_name}, {self.user_id}, {self.user_id_provider}, {self.session_id}, {self.url}, {self.app}, {self.http_user_agent}, {self.status}, {self.http_content_type}, {self.bytes}, {self.duration}, {self.nih_ico}, {self.cadr_name}, {self.user_country_name}, {self.user_org}, {self.user_email}, {self.associated_study}, {self.eRA_commons_id}, {self.user_permission_group}, {self.event_type}"

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

    # Constants
    user_id_provider = "Terra"
    # TODO - MANUALLY PROVIDE THESE, Example: AnVil
    nih_ico = ""
    cadr_name = ""

    # Collect users and dataset/snapshot ids
    populatedNewLogs = []
    emails = set()
    dataset_ids = set()
    snapshot_ids = set()
    for log in get_raw_logs(raw_logs_location):
        newLog = Log()
        newLog._time = log["timestamp"]
        log_message = log["jsonPayload"]["message"]
        for item in log_message.split(","):
            item = item.strip()
            if item.startswith("userId"):
                newLog.user_id = item.split(":")[1].strip()
            elif item.startswith("url"):
                url = item.split(":", 1)[1].strip()
                newLog.url = url
                # regex that matches /datasets/{UUID}
                regexp = re.compile(r'datasets/[0-9a-fA-F-]{36}')
                if regexp.search(url):
                    dataset_id = regexp.search(url).group(0).split("/")[1]
                    dataset_ids.add(dataset_id)
                    newLog.dataset_id = dataset_id
                # regex that matches /snapshots/{UUID}
                regexp = re.compile(r'snapshots/[0-9a-fA-F-]{36}')
                if regexp.search(url):
                    snapshot_id = regexp.search(url).group(0).split("/")[1]
                    snapshot_ids.add(snapshot_id)
                    newLog.snapshot_id = snapshot_id
            elif item.startswith("email"):
                 email = item.split(":")[1].strip()
                 newLog.user_email = email
                 emails.add(email)
            elif item.startswith("institute"):
                newLog.user_org = item.split(":")[1].strip()
            elif item.startswith("status"):
                newLog.status = item.split(":")[1].strip()
            elif item.startswith("srcIP"):
                newLog.src_ip = item.split(":")[1].strip()
            elif item.startswith("destIP"):
                newLog.dest_ip = item.split(":")[1].strip()
            elif item.startswith("destPort"):
                newLog.dest_port = item.split(":")[1].strip()
            elif item.startswith("sessionId"):
                newLog.session_id = item.split(":")[1].strip()
            elif item.startswith("userAgent"):
                newLog.http_user_agent = item.split(":")[1].strip()
            elif item.startswith("contentType"):
                newLog.http_content_type = item.split(":")[1].strip()
            elif item.startswith("bytes"):
                newLog.bytes = item.split(":")[1].strip()
            elif item.startswith("duration"):
                newLog.duration = item.split(":")[1].strip()
            elif item.startswith("method"):
                newLog.method = item.split(":")[1].strip()
        newLog.event_type = event_type_from_url(newLog.url, newLog.method).name
        populatedNewLogs.append(newLog)

    # Get user details from Thurloe/data warehouse
    bq_client = bigquery.Client()
    sm_client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret.
    name = f"projects/terra-datarepo-production/secrets/warehouse-db-name/versions/latest"
    response = sm_client.access_secret_version(request={"name": name})
    WAREHOUSE_DB_NAME = response.payload.data.decode("UTF-8")

    # # Perform a query.
    QUERY = (
        f"""
        SELECT t.USER_ID, e.email, t.Key, t.Value FROM `{WAREHOUSE_DB_NAME}.thurloe` t
       JOIN `{WAREHOUSE_DB_NAME}.email` e ON t.USER_ID = e.USER_ID
        WHERE t.KEY IN ("firstName", "lastName", "institute", "programLocationCountry")
        AND t.USER_ID IN(
            SELECT USER_ID FROM `{WAREHOUSE_DB_NAME}.email`
            WHERE email IN UNNEST(@emails)
        )
        ORDER BY t.USER_ID
        """
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("emails", "STRING", emails)
        ]
    )

    query_job = bq_client.query(QUERY, job_config=job_config)  # API request
    rows = query_job.result()  # Waits for query to finish

    first_names = {}
    last_names = {}
    institutes = {}
    countries = {}
    user_ids = {}
    for row in rows:
        user_ids[row.email] = row.USER_ID
        if (row.Key == "firstName"):
            first_names[row.email] = row.Value
        elif (row.Key == "lastName"):
            last_names[row.email] = row.Value
        elif (row.Key == "institute"):
            institutes[row.email] = row.Value
        elif (row.Key == "programLocationCountry"):
            countries[row.email] = row.Value

    for log in populatedNewLogs:
        if log.user_email != "N/A":
            if log.user_email in first_names:
                log.user_name = first_names[log.user_email] + " " + last_names[log.user_email]
                log.user_country_name = countries[log.user_email]
                log.user_id = user_ids[log.user_email]
                if institutes[log.user_email] is not None:
                    log.user_org = institutes[log.user_email]
            else:
                log.user_name = ""
                log.user_country_name = ""

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

    # Build manual map
    event_type = ""


    # Write to desired format
    outputs = []
    outputs.append(["_time", "src_ip", "dest_ip", "dest_port", "user_name", "user_id", "user_id_provider", "session_id", "method", "url", "app", "http_user_agent", "status", "http_content_type", "bytes", "duration", "nih_ico", "cadr_name", "user_country_name", "user_org", "user_email", "associated_study", "eRA_commons_id", "user_permission_group", "event_type", "dataset_id", "snapshot_id"])
    for log in populatedNewLogs:
        outputs.append([log._time, log.src_ip, log.dest_ip, log.dest_port, log.user_name, log.user_id, user_id_provider, log.session_id, log.method, log.url, log.app, log.http_user_agent, log.status, log.http_content_type, log.bytes, log.duration, log.nih_ico, log.cadr_name, log.user_country_name, log.user_org, log.user_email, log.associated_study, log.eRA_commons_id, log.user_permission_group, log.event_type, log.dataset_id, log.snapshot_id])

    output_filename = f"output/{os.path.basename(args.raw_logs_location).split('.')[0]}_{datetime.datetime.now()}.csv"

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in outputs:
            writer.writerow(row)


    print(f"\n\n\nDONE. Logs written to {output_filename}")


if __name__ == "__main__":
    main()
