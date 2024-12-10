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
        required=True,
        help="The data repo root URL to point to. This is required flag. Examples include `http://localhost:8080` or `https://jade.datarepo-dev.broadinstitute.org`",
    )
    parser.add_argument(
        "--snapshot_id",
        required=True,
        help="snapshot_id to remove member from",
    )
    parser.add_argument(
        "--member_email",
        required=True,
        help="member email to remove from snapshot",
    )
    args = parser.parse_args()
    clients = Clients(args.host)

    snapshot_id = args.snapshot_id
    member_email = args.member_email

    print(f"Removing {member_email} as reader from Snapshot {snapshot_id}")
    clients.snapshots_api.remove_snapshot_policy_member(snapshot_id, "reader", policy_member={"email": member_email})



if __name__ == "__main__":
    main()
