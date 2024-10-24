import argparse
import subprocess
import requests
import time
import csv
from datetime import datetime

"""This script is designed for measuring the performance and response times of different
'snapshotbuilder' endpoints in the process of various performance optimizations for Azure Synapse.

There is a google.sheets log of the endpoints in the 'Engineering notes' folder in Google Drive.

It utilizes the Google Cloud SDK (gcloud) for authentication and authorization purposes and the
requests library for making HTTP requests to the specified endpoints.

The profiling functions are structured to analyze the following endpoints:

 - getSnapshotBuilderCount: Profiles the performance of the getSnapshotBuilderCount endpoint by
sending POST requests with different criteria.s

 - getConceptHierarchy: Profiles the performance of the getConceptHierarchy endpoint by sending
 GET requests with different concept IDs.

 - enumerateConcepts: Profiles the performance of the enumerateConcepts endpoint by sending GET requests
with different domain IDs and search texts.

 - getConceptChildren: Profiles the performance of the getConceptChildren endpoint by sending GET requests with
 different concept IDs.
"""

# Constants
SNAPSHOTS = {
    "gcp": "db064d16-2cd2-4fd0-82c6-026be6f270c3",
    "azure": "c3eb4708-444f-4cbf-a32c-0d3bb93d4819",
}

HOSTS = {
    "local": "http://localhost:8080",
    "dev": "https://jade.datarepo-dev.broadinstitute.org",
}


def run_command(command):
    """
    Runs a shell command and captures the output.

    Args:
        command (str): The shell command to run.

    Returns:
        str: The command output if successful; otherwise, None.
    """
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(
                f"Command '{command}' failed with exit code {result.returncode}: {result.stderr}"
            )
            return None
    except Exception as e:
        print(f"Error executing command: {e}")
        return None


def authenticate():
    """
    Prompts the user to authenticate using gcloud auth login.
    """
    print("Please complete the authentication in the web browser that opens.")
    auth_command = "gcloud auth login"
    run_command(auth_command)


def get_access_token():
    """
    Retrieves the access token using gcloud auth print-access-token.

    Returns:
        str: The access token if successful; otherwise, None.
    """
    token_command = "gcloud auth print-access-token"
    return run_command(token_command)

def get_test_concept_ids():
    with open('concept_ids.csv', newline='') as csvfile:
        concept_reader = csv.reader(csvfile)
        concept_ids_inner = [int(row[0]) for row in concept_reader]
    return concept_ids_inner

def handle_response(response):
    if response.status_code == 200:
        print("Request successful!")
    elif response.status_code == 502:
        print(
            "Request timed out, to get a more accurate time, please use the Logs Explorer at console.cloud.google.com for accurate timings"
        )
    else:
        print(
            f"Request failed with status code {response.status_code}: {response.text}"
        )


# HTTP Request Functions
def make_get_request(endpoint_url, token, concept_id):
    """
    Makes a GET request to the specified endpoint with the given access token and endpoint_url.

    Args:
        endpoint_url (str): The endpoint URL to request.
        token (str): The access token for authentication.
    """
    url = f"{DATAREPO_URL}/api/repository/v1/snapshots/{UUID}/snapshotBuilder/{endpoint_url}"
    headers = {"Authorization": f"Bearer {token}"}
    start_time = time.time()
    try:
        response = requests.get(url, headers=headers)
        end_time = time.time()
        handle_response(response)
        print(f"GET request made to {url}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        if response.status_code == 200:
            f = open(RESULTS_FILE_NAME, "a")
            f.write(f"{url},{concept_id},{end_time - start_time:.2f} seconds\n")
            f.close()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")

    print()
    print()


def make_post_request(endpoint_url, token, body):
    """
    Makes a POST request to the specified endpoint with the given access token and request body.

    Args:
        endpoint_url (str): The endpoint URL to request.
        token (str): The access token for authentication.
        body (dict): The request body to send in the POST request.
    """
    url = f"{DATAREPO_URL}/api/repository/v1/snapshots/{UUID}/snapshotBuilder/{endpoint_url}"
    headers = {
        "Authorization": f"Bearer {token}",
        # The request body is in JSON format
        "Content-Type": "application/json",
    }
    start_time = time.time()
    try:
        response = requests.post(url, headers=headers, json=body)
        end_time = time.time()
        handle_response(response)
        print(f"POST request made to {url} with body: {body}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")

    print()
    print()


# Endpoint Profiling Functions
def profile_get_snapshot_builder_count(token):
    """
    Profiles the getSnapshotBuilderCount endpoint and logs the request and time taken

    Args:
        token (str): The access token for authentication.
    """
    print("Profiling getSnapshotBuilderCount endpoint")

    # Define the bodies for the POST request
    bodies = [
        {
            "cohorts": [
                {
                    "name": "",
                    "criteriaGroups": [
                        {
                            "name": "Group 1",
                            "mustMeet": True,
                            "meetAll": True,
                            "criteria": [
                                {"kind": "range", "id": 0, "low": 1955, "high": 2021},
                                {"kind": "list", "id": 2, "values": [38003563]},
                                {"kind": "list", "id": 1, "values": [8507]},
                            ],
                        }
                    ],
                }
            ]
        },
        {
            "cohorts": [
                {
                    "name": "",
                    "criteriaGroups": [
                        {
                            "name": "Group 1",
                            "mustMeet": False,
                            "meetAll": False,
                            "criteria": [
                                {"kind": "domain", "id": 19, "conceptId": 4042140},
                                {"kind": "domain", "id": 19, "conceptId": 441840},
                                {"kind": "domain", "id": 27, "conceptId": 46234754},
                                {"kind": "list", "id": 3, "values": [8515]},
                            ],
                        }
                    ],
                }
            ]
        },
        {
            "cohorts": [
                {
                    "name": "",
                    "criteriaGroups": [
                        {
                            "name": "Group 1",
                            "mustMeet": True,
                            "meetAll": False,
                            "criteria": [
                                {"kind": "range", "id": 0, "low": 1992, "high": 2021},
                                {"kind": "list", "id": 1, "values": [0]},
                                {"kind": "list", "id": 2, "values": [38003563]},
                                {"kind": "list", "id": 3, "values": [8527]},
                            ],
                        }
                    ],
                }
            ]
        },
    ]

    for body in bodies:
        make_post_request("count", token, body)


def profile_get_concept_hierarchy(token):
    """
    Profiles the getConceptHierarchy endpoint and logs the request and time taken
    Args:
        token (str): The access token for authentication.
    """
    print("Profiling getConceptHierarchy endpoint")
    for concept_id in CONCEPT_IDS:
        make_get_request(f"concepts/{concept_id}/hierarchy", token, concept_id)


def profile_enumerate_concepts(token):
    """
    Profiles the searchConcepts endpoint and logs the request and time taken
    Args:
        token (str): The access token for authentication.
    """
    print("Profiling enumerateConcepts endpoint")
    params = [
        (19, None),
        (19, "cancer"),
        (13, None),
        (13, "ane"),
        (21, None),
        (21, "inches"),
    ]
    for param in params:
        domain_id = param[0]
        filter_text = param[1]
        if filter_text is None:
            url = f"concepts?domainId={domain_id}&filterText="
        else:
            url = f"concepts?domainId={domain_id}&filterText={filter_text}"
        make_get_request(url, token, 1)


def profile_get_concept_children(token):
    """
    Profiles the getConceptChildren endpoint and logs the request and time taken
    Args:
        token (str): The access token for authentication.
    """
    print("Profiling getConceptChildren endpoint")
    for concept_id in CONCEPT_IDS:
        make_get_request(f"concepts/{concept_id}/children", token, concept_id)


# Main function
if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(
        description="Profile endpoints for a specific host and snapshot"
    )

    # Add the arguments
    parser.add_argument(
        "--host",
        type=str,
        choices=HOSTS.keys(),
        default="local",
        help="The host to profile",
    )

    parser.add_argument(
        "--snapshot",
        type=str,
        choices=SNAPSHOTS.keys(),
        default="gcp",
        help="The snapshot to profile",
    )

    parser.add_argument(
        "--authenticate",
        type=bool,
        default=False,
        help="If False, use current gcloud authentication. If True, authenticate with "
             "gcloud auth login.",
    )

    args = parser.parse_args()

    UUID = SNAPSHOTS[args.snapshot]
    DATAREPO_URL = HOSTS[args.host]
    CONCEPT_IDS = get_test_concept_ids()

    # Create new file to write results
    RESULTS_FILE_NAME = f"results_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    f = open(RESULTS_FILE_NAME, "x")

    if args.authenticate:
        authenticate()

    # Obtain access token
    access_token = get_access_token()

    # Define endpoint handlers
    endpoint_handlers = {
        "getSnapshotBuilderCount": profile_get_snapshot_builder_count,
        "getConceptHierarchy": profile_get_concept_hierarchy,
        "enumerateConcepts": profile_enumerate_concepts,
        "getConceptChildren": profile_get_concept_children,
    }

    # List of endpoints to process
    endpoints = [
        "getSnapshotBuilderCount",
        "getConceptHierarchy",
        "enumerateConcepts",
        "getConceptChildren",
    ]

    # Iterate through the endpoints and call the corresponding function
    for endpoint in endpoints:
        if endpoint in endpoint_handlers:
            endpoint_handlers[endpoint](access_token)
        else:
            print(f"Unknown endpoint: {endpoint}")
