import unittest
from collect_logs import event_type_from_url, EventType
import uuid
from parameterized import parameterized

# Run tests with the following command:
# `python3 -m unittest tests/event_type_test.py`

id = uuid.uuid4()
table_name = "tableName123"
column_name = "columnName123"
object_id = f'v1_{id}_{id}'
access_id = "accessId123"
class EventTypeTest(unittest.TestCase):
    @parameterized.expand([
        # Data Access
        (f'/api/repository/v1/datasets/{id}/data/{table_name}', "POST", EventType.DATA_ACCESS),
        (f'/api/repository/v1/datasets/{id}/data/{table_name}/statistics/{column_name}', "POST", EventType.DATA_ACCESS),
        (f'/api/repository/v1/datasets/{id}/files', "GET", EventType.DATA_ACCESS),
        (f'/api/repository/v1/datasets/{id}/files/{id}', "GET", EventType.DATA_ACCESS),
        (f'/api/repository/v1/datasets/{id}/filesystem/objects', "GET", EventType.DATA_ACCESS),
        (f'/api/repository/v1/snapshots/{id}/data/{table_name}', "POST", EventType.DATA_ACCESS),
        (f'/api/repository/v1/snapshots/{id}/data/{table_name}/statistics/{column_name}', "POST", EventType.DATA_ACCESS),
        (f'/api/repository/v1/snapshots/{id}/files', "GET", EventType.DATA_ACCESS),
        (f'/api/repository/v1/snapshots/{id}/export', "GET", EventType.DATA_ACCESS),
        (f'/api/repository/v1/snapshots/{id}/files/{id}', "GET", EventType.DATA_ACCESS),
        (f'/ga4gh/drs/v1/objects/{object_id}', "GET", EventType.DATA_ACCESS),
        (f'/ga4gh/drs/v1/objects/{object_id}/access/{access_id}', "GET", EventType.DATA_ACCESS),
        (f'/ga4gh/drs/v1/objects/{object_id}', "POST", EventType.DATA_ACCESS),
        (f'/ga4gh/drs/v1/objects/{object_id}/access/{access_id}', "POST", EventType.DATA_ACCESS),
         # Data Uploads
        (f'/api/repository/v1/datasets/{id}/ingest', "POST", EventType.DATA_UPLOAD),
        (f'/api/repository/v1/datasets/{id}/files', "POST", EventType.DATA_UPLOAD),
        (f'/api/repository/v1/datasets/{id}/files/bulk', "POST", EventType.DATA_UPLOAD),
        (f'/api/repository/v1/datasets/{id}/files/bulk/array', "POST", EventType.DATA_UPLOAD),
        # Data Deletions
        (f'/api/repository/v1/snapshots/{id}', "DELETE", EventType.DATA_DELETION),
        (f'/api/repository/v1/datasets/{id}', "DELETE", EventType.DATA_DELETION),
        (f'/api/repository/v1/datasets/{id}/deletes', "POST", EventType.DATA_DELETION),
        (f'/api/repository/v1/datasets/{id}/files/{id}', "DELETE", EventType.DATA_DELETION),
        # Other
        (f'/api/repository/v1/snapshots/{id}', "GET", EventType.OTHER),
        (f'/api/repository/v1/datasets/{id}', "GET", EventType.OTHER)
    ])

    def test_determines_data_access_event_for_data_url(self, url, method, expectedEventType):
        self.assertEqual(event_type_from_url(url, method), expectedEventType)

if __name__ == '__main__':
    unittest.main()
