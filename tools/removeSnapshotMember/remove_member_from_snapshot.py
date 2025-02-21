import argparse
import json
import subprocess

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
        self.snapshots_api = SnapshotsApi(api_client=self.api_client)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=True,
        help="The data repo root URL to point to. This is required flag. Examples include `http://localhost:8080` or `https://jade.datarepo-dev.broadinstitute.org`",
    )
    parser.add_argument(
        "--request",
        required=True,
        help="file name for request containing member email, role and list of snapshots",
    )

    args = parser.parse_args()
    clients = Clients(args.host)

    request = args.request

    with open(request) as remove_member_json:
        removal_request = json.load(remove_member_json)
        email = removal_request["email"]
        role = removal_request["role"]
        snapshot_ids = removal_request["snapshotIds"]

        print(f"Removing {email} as {role} from {len(snapshot_ids)} snapshots")
        success_count = 0
        failure_count = 0
        for snapshot_id in snapshot_ids:
            try:
                clients.snapshots_api.delete_snapshot_policy_member(snapshot_id, role,
                                                                    email)
                success_count += 1
            except Exception as e:
                print(
                    f"Error: Could not remove {email} from {snapshot_id}; Exception: {e}")
                failure_count += 1
                continue
        print(
            f"DONE. Successfully removed {success_count} members from {len(snapshot_ids)} snapshots. {failure_count} failed.")


if __name__ == "__main__":
    main()
