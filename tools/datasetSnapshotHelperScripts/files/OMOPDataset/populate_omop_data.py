###
# This script is used to generate JSON files with tabular OMOP data with a small test subset of OMOP data
# It queries the BigQuery dataset and writes the results to json files to be used in the setup_tdr_resources.py script
# (1) We start with the person table and pull a small subset of person ids
# (2) Then we retrieve entries the occurrence tables (procedure_occurrence, condition_occurrence, drug_exposure) that map to the selected person ids,
#       noting the referenced concept_ids in these entries
# (3) We pull all the records from some of the reference tables (right now, just the domain table, but we could include others)
# (4) We pull entries from the concept table that match referenced concept_ids from the occurrence tables
# (5) We populate the concept_ancestor table with the ancestor_concept_id pulled from occurrence table's domain id and
#       then the descendant_concept_id contains all the concepts form the concept table.
#       This is the only table that we are fabricating.
# (6) We write the results to json files
# (7) We use the json files in the setup_tdr_resources.py script to populate a test environment with the OMOP data
#
# How to use this script
# Note: this script overwrites the json files each time it is run
# (1) Set the gcp_with_access variable to a google project that you have job big query access on
# (2) Set the gcp_project and gcp_dataset variables to the project and dataset that you want to pull data from
# (3) Set the person_limit variable to the number of person records you want to pull.
#       Note: Increasing this will very quickly increase the size of the tables, especially the concept table.

# Running the script
#1. If you need to set up a virtual python environment, you can run the following commands:
#    * `python3 -m venv c:/path/to/myenv`
#    * `source c:/path/to/myenv/bin/activate`
#2. `cd jade-data-repo/tools/datasetSnapshotHelperScripts`
#3. `pip3 install -r requirements.txt`
#4. `gcloud auth login <user>`
#5. `cd files/OMOPDataset'
#6  `python3 populate_omop_data.py`
#7  `cd ../..` (go back to the datasetSnapshotHelperScripts directory)
#5. `python3 setup_tdr_resources.py --host <datarepo_url> --datasets datarepo_omop_datasets.json --profile_id <profile_id>`



# Troubleshooting
# If you run into an authenticating issue, you may need to set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your application default credentials file. You can do this by running the following command:
# gcloud auth application-default login
# export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json

# Future improvement - We could add more a more automatic way to authenticate before running this app. Some relevant links are below:
# https://developers.google.com/people/quickstart/python
# https://google-auth.readthedocs.io/en/master/user-guide.html


from google.cloud import bigquery
import json
import re
import os


## You'll need to set this to a DSP google project that you have job big query access on
## If you have steward access on the TDR dataset, then that should be sufficient
gcp_with_access = "datarepo-dev-8c33d3b5"
## Location of the source OMOP data
gcp_project = "datarepo-dev-8c33d3b5"
gcp_dataset = "datarepo_AXIN_OMOP_Data_20230810"

person_limit = 2
person_ids = set()
concept_ids = set()
concept_ancestor_file_path = "concept_ancestor.json"

def main():
    # Manually remove existing concept_ancestor file since we just append to it
    if os.path.isfile(concept_ancestor_file_path):
        os.remove(concept_ancestor_file_path)

    client = bigquery.Client(project=gcp_with_access)
    # person
    query_root_person_table(client)

    # Occurrence Tables
    # To add later - "observation", "observation_period",  "device_exposure"
    query_table_where_person_id_record_concepts(client, "procedure_occurrence", 49)
    query_table_where_person_id_record_concepts(client, "condition_occurrence", 19)
    query_table_where_person_id_record_concepts(client, "drug_exposure", 13)

    # Static Reference Tables
    query_table_all_results(client, "domain")
    # Tables to add later - another reference table - "vocabulary", "relationship", "concept_class"

    # Concept
    query_table_where_concepts(client, "concept", "concept_id")

def query_table_all_results(client, table_name):
    query = f"Select * FROM `{gcp_project}.{gcp_dataset}.{table_name}`;"
    query_table(client, query, table_name)

def query_root_person_table(client):
    person_table = "person"
    query = f"Select * FROM `{gcp_project}.{gcp_dataset}.{person_table}` LIMIT {person_limit};"
    records = query_table(client, query, person_table)
    for record in records:
        person_ids.add(record["person_id"])

def query_table_where_person_id_record_concepts(client, table_name, domain_id):
    person_ids_str = ', '.join(str(id) for id in person_ids)
    query = f"Select * FROM `{gcp_project}.{gcp_dataset}.{table_name}` WHERE person_id IN ({person_ids_str});"
    records = query_table(client, query, table_name)
    # Loosely match on "concept_id" colum name since the column name varies between tables that maps to concept_id
    table_concept_ids = set()
    for record in records:
        for i in record.keys():
            if(i.find("concept_id") != -1):
                item = record[i]
                if item is not None:
                    table_concept_ids.add(record[i])
                    concept_ids.add(record[i])
    with open(concept_ancestor_file_path, "a") as file:
        for concept_id in table_concept_ids:
            json_obj_formatted = format_content(str({"ancestor_concept_id": domain_id, "descendant_concept_id": concept_id, "min_levels_of_separation": 1, "max_levels_of_separation": 1}) + "\n")
            file.write(json_obj_formatted)

def query_table_where_concepts(client, table_name, concept_id_field):
    concept_ids_str = ', '.join(str(id) for id in concept_ids)
    query = f"Select * FROM `{gcp_project}.{gcp_dataset}.{table_name}` WHERE {concept_id_field} IN ({concept_ids_str});"
    query_table(client, query, table_name)

def query_table(client, query, table_name):
    query_job = client.query(query)
    rows = query_job.result()

    records = [dict(row) for row in rows]
    json_obj = json.dumps(str(records))
    json_obj_formatted = format_json(json_obj)

    # Write rows to json file
    with open(f"{table_name}.json", "w") as file:
        file.write(json_obj_formatted)
    return records

def format_json(json_obj):
    json_obj_formatted = format_content(json_obj)
    # Remove outer list brackets and quotes
    json_obj_no_brackets = json_obj_formatted[2:-2]
    # Add newlines between json objects instead of commas
    return json_obj_no_brackets.replace("}, {", "}\n{")

def format_content(json_obj):
    # regex to match "datetime.date(2009, 7, 15)" and replace with "2009-07-15"
    json_obj_with_dates = re.sub(r"datetime.date\((\d+), (\d+), (\d+)\)", r"'\1-\2-\3'", json_obj)
    # Handle \" in json
    json_obj_switch_to_single_quotes = json_obj_with_dates.replace("\\\"", "'")
    # Switch to double quotes
    json_obj_correctly_quoted = json_obj_switch_to_single_quotes.replace("{'", "{\"").replace("'}", "\"}").replace(", '", ", \"").replace("',", "\",").replace("':", "\":").replace(": '", ": \"")
    # Replace "None" with "null"
    return json_obj_correctly_quoted.replace("None", "null")

if __name__ == "__main__":
    main()
