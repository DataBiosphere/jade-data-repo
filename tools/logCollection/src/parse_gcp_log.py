import json
import os
import re

from .log_class import LogClass
from .categorize_event_type import event_type_from_url

def get_raw_logs(filename):
    with open(os.path.join("rawLogs", filename)) as f:
        return json.load(f)

def parse_gcp_log(raw_logs_location, emails, dataset_ids, snapshot_ids):
    populated_new_logs = []
    for log in get_raw_logs(raw_logs_location):
        new_log = LogClass()
        new_log._time = log["timestamp"]
        log_message = log["jsonPayload"]["message"]
        for item in log_message.split(","):
            item = item.strip()
            if item.startswith("userId"):
                new_log.user_id = item.split(":")[1].strip()
            elif item.startswith("url"):
                url = item.split(":", 1)[1].strip()
                new_log.url = url
                # regex that matches /datasets/{UUID}
                regexp = re.compile(r'datasets/[0-9a-fA-F-]{36}')
                if regexp.search(url):
                    dataset_id = regexp.search(url).group(0).split("/")[1]
                    dataset_ids.add(dataset_id)
                    new_log.dataset_id = dataset_id
                # regex that matches /snapshots/{UUID}
                regexp = re.compile(r'snapshots/[0-9a-fA-F-]{36}')
                if regexp.search(url):
                    snapshot_id = regexp.search(url).group(0).split("/")[1]
                    snapshot_ids.add(snapshot_id)
                    new_log.snapshot_id = snapshot_id
            elif item.startswith("email"):
                email = item.split(":")[1].strip()
                new_log.user_email = email
                emails.add(email)
            elif item.startswith("institute"):
                new_log.user_org = item.split(":")[1].strip()
            elif item.startswith("status"):
                new_log.status = item.split(":")[1].strip()
            elif item.startswith("srcIP"):
                new_log.src_ip = item.split(":")[1].strip()
            elif item.startswith("destIP"):
                new_log.dest_ip = item.split(":")[1].strip()
            elif item.startswith("destPort"):
                new_log.dest_port = item.split(":")[1].strip()
            elif item.startswith("sessionId"):
                new_log.session_id = item.split(":")[1].strip()
            elif item.startswith("userAgent"):
                new_log.http_user_agent = item.split(":")[1].strip()
            elif item.startswith("contentType"):
                new_log.http_content_type = item.split(":")[1].strip()
            elif item.startswith("bytes"):
                new_log.bytes = item.split(":")[1].strip()
            elif item.startswith("duration"):
                new_log.duration = item.split(":")[1].strip()
            elif item.startswith("method"):
                new_log.method = item.split(":")[1].strip()
        new_log.event_type = event_type_from_url(new_log.url, new_log.method).name
        populated_new_logs.append(new_log)
    return populated_new_logs
