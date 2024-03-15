from data_repo_client import Configuration, ApiClient, ProfilesApi, DatasetsApi, SnapshotsApi, JobsApi
import argparse
import subprocess
import json
import os
import time
import uuid


class Clients:
    def __init__(self, host):
        config = Configuration()
        config.host = host
        token_output = subprocess.run(['gcloud', 'auth', 'print-access-token'], capture_output=True)
        config.access_token = token_output.stdout.decode("UTF-8").strip()
        self.api_client = ApiClient(configuration=config)

        self.profiles_api = ProfilesApi(api_client=self.api_client)
        self.datasets_api = DatasetsApi(api_client=self.api_client)
        self.snapshots_api = SnapshotsApi(api_client=self.api_client)
        self.jobs_api = JobsApi(api_client=self.api_client)


def wait_for_job(clients, job_model):
    result = job_model
    while True:
        if result is None or result.job_status == "running":
            time.sleep(10)
            print(f"Waiting for job {job_model.id} to finish")
            result = clients.jobs_api.retrieve_job(job_model.id)
        elif result.job_status == 'failed':
            result = clients.jobs_api.retrieve_job_result(job_model.id)
            raise f"Could not complete job with id {job_model.id}, got result {result}"
        elif result.job_status == "succeeded":
            print(f"Job {job_model.id} succeeded")
            result = clients.jobs_api.retrieve_job_result(job_model.id)
            return result
        else:
            raise "Unrecognized job state %s" % result.job_status

# For dataset_ingest requests, each line in file is a json object
# We need to convert to this to an array of json objects
def convert_to_json_array(table_csv):
    records = ""
    for row in table_csv.readlines():
        records += row.strip("\n") + ","
    return json.loads("[" + records.strip(",") + "]")

def dataset_ingest_array(clients, dataset_id, dataset_to_upload):
    for table in dataset_to_upload['tables']:
        with open(os.path.join("files", dataset_to_upload["schema"],
                               f"{table}.json")) as table_csv:
            recordsArray = convert_to_json_array(table_csv)
            if len(recordsArray) > 0:
                ingest_request = {
                    "format": "array",
                    "records":recordsArray,
                    "table": table
                }
                print(f"Ingesting data into {dataset_to_upload['name']}/{table}")
                wait_for_job(clients, clients.datasets_api.ingest_dataset(dataset_id, ingest=ingest_request))
            else:
                print(f"Skipping ingest of {dataset_to_upload['name']}/{table} because it is empty")

def create_billing_profile(clients, add_jade_stewards, cloud_platform):
    billing_profile_file_name = "billing_profile.json"
    if cloud_platform == "azure":
        billing_profile_file_name = "azure_billing_profile.json"
    with open(os.path.join("files", billing_profile_file_name)) as billing_profile_json:
        billing_profile_request = json.load(billing_profile_json)
        profile_id = str(uuid.uuid4())
        billing_profile_request['id'] = profile_id
        billing_profile_request['profileName'] = billing_profile_request['profileName'] + f'_{profile_id}'
        print(f"Creating billing profile with id: {profile_id}")
        profile = wait_for_job(clients, clients.profiles_api.create_profile(billing_profile_request=billing_profile_request))
        if add_jade_stewards:
            add_billing_profile_members(clients, profile_id)
        return profile


def add_billing_profile_members(clients, profile_id):
    clients.profiles_api.add_profile_policy_member(profile_id,
                                                   "owner",
                                                   {'email': 'JadeStewards-dev@dev.test.firecloud.org'})


def dataset_ingest_json(clients, dataset_id, dataset_to_upload):
    for table in dataset_to_upload['tables']:
        with open(os.path.join("files", dataset_to_upload["schema"],
                               f"{table}.{dataset_to_upload['format']}")) as table_csv:
            upload_prefix = dataset_to_upload['upload_prefix']
            ingest_request = {
                "format": "json",
                "path": f"{upload_prefix}/{table}.json",
                "table": table
            }
            print(f"Ingesting data into {dataset_to_upload['name']}/{table}")
            wait_for_job(clients, clients.datasets_api.ingest_dataset(dataset_id, ingest=ingest_request))

def add_dataset_policy_members(clients, dataset_id, dataset_to_upload):
    for steward in dataset_to_upload['stewards']:
        print(f"Adding {steward} as a steward")
        clients.datasets_api.add_dataset_policy_member(dataset_id, "steward", policy_member={"email": steward})
    for custodian in dataset_to_upload['custodians']:
        print(f"Adding {custodian} as a custodian")
        clients.datasets_api.add_dataset_policy_member(dataset_id, "custodian", policy_member={"email": custodian})
    for snapshot_creator in dataset_to_upload['snapshot_creators']:
        print(f"Adding {snapshot_creator} as a snapshot_creator")
        clients.datasets_api.add_dataset_policy_member(dataset_id, "snapshot_creator", policy_member={"email": snapshot_creator})


def create_dataset(clients, dataset_to_upload, profile_id):
    dataset_name = dataset_to_upload['name']

    dataset = None
    with open(os.path.join("files", dataset_to_upload["schema"], "dataset_schema.json")) as dataset_schema_json:
        dataset_request = json.load(dataset_schema_json)
        dataset_request['cloudPlatform'] = dataset_to_upload["cloud_platform"]
        dataset_request['name'] = dataset_name
        dataset_request['defaultProfileId'] = profile_id
        print(f"Creating dataset {dataset_name}")
        dataset = wait_for_job(clients, clients.datasets_api.create_dataset(dataset=dataset_request))
        print(f"Created dataset {dataset_name} with id: {dataset['id']}")

    if dataset_to_upload['format'] == "json":
        dataset_ingest_json(clients, dataset['id'], dataset_to_upload)
    elif dataset_to_upload['format'] == "array":
        dataset_ingest_array(clients, dataset['id'], dataset_to_upload)
    else:
        raise Exception("Must specify the ingest format. Right now we support json and array")

    add_dataset_policy_members(clients, dataset['id'], dataset_to_upload)
    return dataset


def get_datasets_to_upload(filename):
    with open(filename) as f:
        return json.load(f)


def add_snapshot_policy_members(clients, snapshot_id, snapshot_to_upload):
    for steward in snapshot_to_upload.get('stewards', []):
        print(f"Adding {steward} as a steward")
        clients.snapshots_api.add_snapshot_policy_member(snapshot_id, "steward", policy_member={"email": steward})
    for reader in snapshot_to_upload.get('readers', []):
        print(f"Adding {reader} as a reader")
        clients.snapshots_api.add_snapshot_policy_member(snapshot_id, "reader", policy_member={"email": reader})
    for discoverer in snapshot_to_upload.get('discoverers', []):
        print(f"Adding {discoverer} as a discoverer")
        clients.snapshots_api.add_snapshot_policy_member(snapshot_id, "discoverer", policy_member={"email": discoverer})


def create_snapshots(clients, dataset_name, snapshots, profile_id):
    snapshot_ids = []
    for snapshot_to_upload in snapshots:
        count = snapshot_to_upload.get('count', 1)
        for i in range(count):
            snapshot_name = f"{snapshot_to_upload['name']}{i + 1}"
            snapshot_request = {
                'name': snapshot_name,
                'description': snapshot_to_upload['description'],
                'contents': [{'datasetName': dataset_name,
                              'mode': 'byFullView'}],
                'profileId': profile_id
            }
            snapshot = wait_for_job(clients, clients.snapshots_api.create_snapshot(snapshot=snapshot_request))
            print(f"Created snapshot {snapshot_name} with id: {snapshot['id']}")
            add_snapshot_policy_members(clients, snapshot['id'], snapshot_to_upload)
            snapshot_ids.append(snapshot['id'])
    return snapshot_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='https://jade-4.datarepo-integration.broadinstitute.org')
    parser.add_argument('--datasets', default='./suites/datarepo_datasets.json')
    parser.add_argument('--gcp_profile_id')
    parser.add_argument('--azure_profile_id')
    args = parser.parse_args()
    clients = Clients(args.host)

    add_jade_stewards = 'dev' in args.host or 'integration' in args.host
    gcp_profile_id = args.gcp_profile_id
    azure_profile_id = args.azure_profile_id

    outputs = []
    for dataset_to_upload in get_datasets_to_upload(args.datasets):
        dataset_cloud_platform = dataset_to_upload['cloud_platform']
        if dataset_cloud_platform == 'gcp':
            profile_id = gcp_profile_id
        elif dataset_cloud_platform == 'azure':
            profile_id = azure_profile_id
        else:
            raise Exception("Billing profile create not yet supported for cloud platform: " + dataset_cloud_platform)
        if profile_id is None:
            profile_job_response = create_billing_profile(clients, add_jade_stewards, dataset_cloud_platform)
            profile_id = profile_job_response['id']
        created_dataset = create_dataset(clients, dataset_to_upload, profile_id)
        dataset_name = created_dataset['name']
        output_ids = {dataset_name: {'dataset_id': created_dataset['id']}}
        if dataset_to_upload.get('snapshots'):
            snapshot_ids = create_snapshots(clients, dataset_to_upload['name'], dataset_to_upload['snapshots'],
                                            profile_id)
            output_ids[dataset_name]['snapshot_ids'] = snapshot_ids
            outputs.append(output_ids)

    output_filename = f"{os.path.basename(args.datasets).split('.')[0]}_outputs.json"
    with open(output_filename, 'w') as f:
        json.dump(outputs, f)


if __name__ == "__main__":
    main()
