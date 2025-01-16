import argparse
import json
import os
import subprocess
import time
import uuid

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=False,
        default="https://data.terra.bio/",
        help="The data repo root URL to point to. This is required flag. Examples include `http://localhost:8080` or `https://jade.datarepo-dev.broadinstitute.org`",
    )
    parser.add_argument(
        "--request",
        required=False,
        default="reader_datasetId.json",
        help="file name for request containing role and list of datasetIds",
    )

    args = parser.parse_args()
    clients = Clients(args.host)

    request = args.request

    with open(request) as check_permission_json:
        check_permission_request = json.load(check_permission_json)
        role = check_permission_request["role"]
        dataset_ids = check_permission_request["datasetIds"]

        print(f"Checking reader permissions on snapshots from datasetIds: {dataset_ids}")
        result = clients.snapshots_api.enumerate_snapshots(dataset_ids=dataset_ids, limit=3000)
        snapshots = result.items
        print("{} snapshots found".format(len(snapshots)))
        for snapshot in snapshots:
            #print(f"Checking reader permission on {snapshot.id}")
            policies = clients.snapshots_api.retrieve_snapshot_policies(snapshot.id)
            if "SDTDRGRP-SDPR-X@firecloud.org" not in [policy for policy in policies.policies if policy.name == role][0].members:
                print(f"Missing reader permission on {snapshot.id}")








if __name__ == "__main__":
    main()
