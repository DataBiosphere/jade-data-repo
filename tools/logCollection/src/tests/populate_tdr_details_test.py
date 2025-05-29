import unittest
from ..populate_tdr_details import retrieve_snapshot_auth_domain, TDRClients, populate_snapshot_user_permission_group_and_studies, retrieve_snapshot_via_enumerate, retrieve_dataset_via_enumerate, populate_dataset_studies
from ..log_class import LogClass
# Run tests with the following command:
# `python3 -m unittest src/tests/populate_tdr_details_test.py`

# example snapshot that firecloud accounts do not have direct access to,
# but still can access the policies endpoint
# Full_View_Snapshot_of_NonStewardDataset_1746106084627
# has auth domain
prod_example_snapshot_id = "49c010f6-1112-4839-b87d-8dfcfe5407a7"
prod_example_snapshot_auth_domain = "AXIN_Testing_Group"
prod_example_dataset_id = "1e81193f-73f7-4923-91ed-e302a4b4eea5"
host = "https://data.terra.bio"
clients = TDRClients(host)
class PopulateTdrTest(unittest.TestCase):
    def test_retrieve_snapshot_auth_domain(self):
        self.assertEqual(retrieve_snapshot_auth_domain(clients, prod_example_snapshot_id), [prod_example_snapshot_auth_domain])

    def test_retrieve_snapshot_via_enumerate(self):
        snapshot = retrieve_snapshot_via_enumerate(clients, prod_example_snapshot_id)
        self.assertEqual(snapshot.id, prod_example_snapshot_id)

    def test_retrieve_dataset_via_enumerate(self):
        dataset = retrieve_dataset_via_enumerate(clients, prod_example_dataset_id)
        self.assertEqual(dataset.id, prod_example_dataset_id)


    def test_populate_user_permission_group(self):
        # populate test data
        populated_new_logs = []
        example_log = LogClass()
        example_log.snapshot_id = prod_example_snapshot_id
        populated_new_logs.append(example_log)
        snapshot_ids = {prod_example_snapshot_id}
        # run populate method
        populate_snapshot_user_permission_group_and_studies(populated_new_logs, snapshot_ids, host)
        # confirm auth domains were added to the logs
        self.assertEqual(populated_new_logs[0].user_permission_group, [prod_example_snapshot_auth_domain])
        # empty list since we're testing against prod, but manually confirmed duos id gets added
        self.assertEqual(populated_new_logs[0].associated_study, [])

    def test_populate_dataset_studies(self):
        # populate test data
        populated_new_logs = []
        example_log = LogClass()
        example_log.dataset_id = prod_example_dataset_id
        populated_new_logs.append(example_log)
        dataset_ids = {prod_example_dataset_id}
        # run populate method
        populate_dataset_studies(populated_new_logs, dataset_ids, host)
        # None since we're testing against prod
        self.assertIsNone(populated_new_logs[0].associated_study)

if __name__ == '__main__':
    unittest.main()
