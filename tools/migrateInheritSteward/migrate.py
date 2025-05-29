import argparse
import json
import os
import subprocess
import time
import uuid

from data_repo_client import (
    Configuration,
    ApiClient,
    DatasetsApi,
    JobsApi,
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

        self.datasets_api = DatasetsApi(api_client=self.api_client)
        self.jobs_api = JobsApi(api_client=self.api_client)


def wait_for_job(clients, job_model):
    result = clients.jobs_api.retrieve_job(job_model.id)
    while True:
        if result is None or result.job_status == "running":
            time.sleep(10)
            print(f"Waiting for job {job_model.id} to finish")
            result = clients.jobs_api.retrieve_job(job_model.id)
        elif result.job_status == "failed":
            print(job_model.id)
            try:
                result = clients.jobs_api.retrieve_job_result(job_model.id)
            except Exception as e:
                return e.body
        elif result.job_status == "succeeded":
            print(f"Job succeeded {job_model.id}: {job_model.description}")
            result = clients.jobs_api.retrieve_job_result(job_model.id)
            print(result)
            return result
        else:
            raise "Unrecognized job state %s" % result.job_status


def wait_for_jobs(clients, outputs, jobs):
    for job in jobs:
        outputs.append(wait_for_job(clients, job))


def get_datasets(filename):
    with open(filename) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=True,
        help="The data repo root URL to point to. This is required flag. Examples include `http://localhost:8080` or `https://jade.datarepo-dev.broadinstitute.org`",
    )
    parser.add_argument(
        "--datasets",
        required=True,
        help="A file pointer to the Dataset IDs to flip inherit steward for",
    )
    parser.add_argument(
        "--inherit_steward",
        help="What to set the new value for inherit steward to. This is a boolean value.",
    )
    args = parser.parse_args()
    clients = Clients(args.host)

    outputs = []
    jobs = []
    for dataset_id in get_datasets(args.datasets).get("datasets", []):
        print(f"Updating dataset {dataset_id} to inherit steward {args.inherit_steward}")
        jobs.append(clients.datasets_api.set_inherit_steward(dataset_id, body=args.inherit_steward))
    wait_for_jobs(clients, outputs, jobs)
    output_filename = f"{os.path.basename(args.datasets).split('.')[0]}_outputs.json"
    with open(output_filename, "w") as f:
        print(f"Writing outputs to {output_filename}")
        json.dump(outputs, f)


if __name__ == "__main__":
    main()
