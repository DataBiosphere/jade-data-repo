from enum import Enum
import re
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
