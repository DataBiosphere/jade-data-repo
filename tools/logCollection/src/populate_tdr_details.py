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


class TDRClients:
    def __init__(self, host):
        config = Configuration()
        config.host = host
        token_output = subprocess.run(
            ["gcloud", "auth", "print-access-token"], capture_output=True
        )
        config.access_token = token_output.stdout.decode("UTF-8").strip()
        self.api_client = ApiClient(configuration=config)

        self.datasets_api = DatasetsApi(api_client=self.api_client)
        self.snapshots_api = SnapshotsApi(api_client=self.api_client)


def populate_snapshot_user_permission_group_and_studies(populated_new_logs, snapshot_ids, host):
    clients = TDRClients(host)
    for snapshot_id in snapshot_ids:
        user_permission_group = retrieve_snapshot_auth_domain(clients, snapshot_id)
        snapshot = retrieve_snapshot_via_enumerate(clients, snapshot_id)
        for log in populated_new_logs:
            if log.snapshot_id == snapshot_id and snapshot is not None:
                log.user_permission_group = user_permission_group
                log.associated_study = list(filter(None,[snapshot.phs_id, snapshot.duos_id]))

def populate_dataset_studies(populated_new_logs, dataset_ids, host):
    clients = TDRClients(host)
    for dataset_id in dataset_ids:
        dataset = retrieve_dataset_via_enumerate(clients, dataset_id)
        for log in populated_new_logs:
            if log.dataset_id == dataset_id and dataset is not None:
                log.associated_study = dataset.phs_id

### TDR API calls ###
def retrieve_snapshot_auth_domain(clients, snapshot_id):
    policy_response = clients.snapshots_api.retrieve_snapshot_policies(snapshot_id)
    return policy_response.auth_domain


# We want a TDR admin to be able to run this on any snapshot
# So, we can't directly use the retrieve snapshot endpoint
# We need to use the enumerate snapshot endpoint with a filter for the snapshot id
def retrieve_snapshot_via_enumerate(clients, snapshot_id):
    snapshots_response = clients.snapshots_api.enumerate_snapshots(filter=snapshot_id)
    for snapshot in snapshots_response.items:
        if snapshot.id == snapshot_id:
            return snapshot
    return None


def retrieve_dataset_via_enumerate(clients, dataset_id):
    datasets_response = clients.datasets_api.enumerate_datasets(filter=dataset_id)
    for dataset in datasets_response.items:
        if dataset.id == dataset_id:
            return dataset
    return None
