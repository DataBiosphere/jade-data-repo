import subprocess
import requests
import time

# Constants
UUID = "c672c3c9-ab54-4e19-827c-f2af329da814"
DATAREPO_URL = "https://jade.datarepo-dev.broadinstitute.org"


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
            raise Exception(
                f"Command failed with exit code {result.returncode}: {result.stderr}"
            )
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


# HTTP Request Functions


def make_get_request(endpoint_url, token):
    """
    Makes a GET request to the specified endpoint with the given access token.

    Args:
        endpoint_url (str): The endpoint URL to request.
        token (str): The access token for authentication.

    Returns:
        float: The time taken for the request in seconds.
    """
    url = f"{DATAREPO_URL}/api/repository/v1/datasets/{UUID}/snapshotBuilder/{endpoint_url}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        start_time = time.time()
        response = requests.get(url, headers=headers)
        end_time = time.time()

        total_time = end_time - start_time

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

        return total_time
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return None


def make_post_request(endpoint_url, token, body):
    """
    Makes a POST request to the specified endpoint with the given access token and request body.

    Args:
        endpoint_url (str): The endpoint URL to request.
        token (str): The access token for authentication.
        body (dict): The request body to send in the POST request.

    Returns:
        tuple: A tuple containing the response object and the time taken for the request in seconds.
    """
    url = f"{DATAREPO_URL}/api/repository/v1/datasets/{UUID}/snapshotBuilder/{endpoint_url}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",  # Indicating the request body is in JSON format
    }

    try:
        start_time = time.time()
        response = requests.post(url, headers=headers, json=body)
        end_time = time.time()

        time_taken = end_time - start_time

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

        return time_taken

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return None

g
# Endpoint Profiling Functions
def profile_get_snapshot_builder_count(token):
    """
    Profiles the getSnapshotBuilderCount endpoint and returns the time taken.

    Args:
        token (str): The access token for authentication.

    Returns:
        str: A string indicating the average time taken for the request.
    """
    print("Profiling getSnapshotBuilderCount endpoint")

    # Define the bodies for the POST request
    bodys = [
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
                            "mustMeet": True,
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

    time_length = 0
    for body in bodys:
        _, time_taken = make_post_request("count", token, body)
        time_length += time_taken

    return f"Time taken: {time_length / len(bodys)} seconds"


def profile_get_concept_hierarchy(token):
    """
    Profiles the getConceptHierarchy endpoint and returns the average time taken.

    Args:
        token (str): The access token for authentication.

    Returns:
        str: A string indicating the average time taken for the request.
    """
    print("Profiling getConceptHierarchy endpoint")

    concept_ids = [4180169, 4027384, 4029205]
    time_length = 0

    for concept_id in concept_ids:
        time_length += make_get_request(f"conceptHierarchy/{concept_id}", token)

    return f"Time taken: {time_length / len(concept_ids)} seconds"


def profile_search_concepts(token):
    """
    Profiles the searchConcepts endpoint and returns the average time taken.

    Args:
        token (str): The access token for authentication.

    Returns:
        str: A string indicating the average time taken for the request.
    """
    print("Profiling searchConcepts endpoint")

    params = [
        (19, None),
        (19, "cancer"),
        (13, None),
        (13, "ane"),
        (21, None),
        (21, "inches"),
    ]

    time_length = 0

    for param in params:
        domain_id = param[0]
        search_text = param[1]

        if search_text is None:
            url = f"concepts/{domain_id}/search?/searchText="
        else:
            url = f"concepts/{domain_id}/search?/searchText={search_text}"

        time_length += make_get_request(url, token)

    return f"Time taken: {time_length / len(params)} seconds"


def profile_get_concepts(token):
    """
    Profiles the getConcepts endpoint and returns the average time taken.

    Args:
        token (str): The access token for authentication.

    Returns:
        str: A string indicating the average time taken for the request.
    """
    print("Profiling getConcepts endpoint")

    concept_ids = [443883, 4042140, 4103320]
    time_length = 0

    for concept_id in concept_ids:
        time_length += make_get_request(f"concepts/{concept_id}", token)

    return f"Time taken: {time_length / len(concept_ids)} seconds"


# Main function
if __name__ == "__main__":
    # Uncomment the following line to authenticate if needed
    authenticate()

    # Obtain access token
    access_token = get_access_token()

    # Define endpoint handlers

    endpoint_handlers = {
        "getSnapshotBuilderCount": profile_get_snapshot_builder_count,
        "getConceptHierarchy": profile_get_concept_hierarchy,
        "searchConcepts": profile_search_concepts,
        "getConcepts": profile_get_concepts,
    }

    # List of endpoints to process
    endpoints = [
        "getSnapshotBuilderCount",
        "getConceptHierarchy",
        "searchConcepts",
        "getConcepts",
    ]

    # Iterate through the endpoints and call the corresponding function
    for endpoint in endpoints:
        if endpoint in endpoint_handlers:
            print(endpoint_handlers[endpoint](access_token))
        else:
            print(f"Unknown endpoint: {endpoint}")
