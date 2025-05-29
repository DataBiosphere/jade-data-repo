import argparse
import os
import csv
import datetime

from src.parse_gcp_log import parse_gcp_log
from src.populate_user_details_from_data_warehouse import populate_user_details
from src.populate_tdr_details import populate_snapshot_user_permission_group_and_studies, populate_dataset_studies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=False,
        default="https://data.terra.bio",
        help="The data repo root URL to point to. Defaults to production TDR (https://data.terra.bio).`",
    )
    parser.add_argument(
        "--raw_logs_location",
        required=True,
        help="A file pointer to the raw logs to convert. This is required flag. Available Options: "
             + ", ".join(os.listdir("./rawLogs/")),
    )
    args = parser.parse_args()
    raw_logs_location = args.raw_logs_location
    host = args.host

    emails = set()
    dataset_ids = set()
    snapshot_ids = set()

    populated_new_logs = parse_gcp_log(raw_logs_location, emails, dataset_ids, snapshot_ids)
    # Get user details from Thurloe/data warehouse
    populate_user_details(populated_new_logs, emails)

    # For Snapshots, get auth domains from TDR to populate the `user_permission_group` field
    # and get PHSIds and DUOS Ids from TDR to populate the `associated_study` field
    populate_snapshot_user_permission_group_and_studies(populated_new_logs, snapshot_ids, host)

    # For Datasets, get PHSIds from TDR
    populate_dataset_studies(populated_new_logs, dataset_ids, host)

    # Pull ERA commmons id
    eRA_commons_id = ""
    # Not yet relevant


    # Write to desired format
    outputs = []
    outputs.append(["_time", "src_ip", "dest_ip", "dest_port", "user_name", "user_id", "user_id_provider", "session_id", "method", "url", "app", "http_user_agent", "status", "http_content_type", "bytes", "duration", "nih_ico", "cadr_name", "user_country_name", "user_org", "user_email", "associated_study", "eRA_commons_id", "user_permission_group", "event_type", "dataset_id", "snapshot_id"])
    for log in populated_new_logs:
        outputs.append([log._time, log.src_ip, log.dest_ip, log.dest_port, log.user_name, log.user_id, log.user_id_provider, log.session_id, log.method, log.url, log.app, log.http_user_agent, log.status, log.http_content_type, log.bytes, log.duration, log.nih_ico, log.cadr_name, log.user_country_name, log.user_org, log.user_email, log.associated_study, log.eRA_commons_id, log.user_permission_group, log.event_type, log.dataset_id, log.snapshot_id])

    output_filename = f"output/{os.path.basename(args.raw_logs_location).split('.')[0]}_{datetime.datetime.now()}.csv"

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in outputs:
            writer.writerow(row)


    print(f"\n\n\nDONE. Logs written to {output_filename}")

if __name__ == "__main__":
    main()
