The `setup_tdr_resources.py` script can be used to easily create datasets and snapshots in the Data Repo.

### Usage:
1. If you need to set up a virtual python environment, you can run the following commands:
    * `python3 -m venv c:/path/to/myenv`
    * `source c:/path/to/myenv/bin/activate`
2. `cd jade-data-repo/tools/datasetSnapshotHelperScripts`
3. `pip3 install -r requirements.txt`
4. `gcloud auth login <user>`
5. `python3 setup_tdr_resources.py --host <datarepo_url> --datasets <datasets_to_create_json_file> --profile_id <profile_id>`

The script outputs are written to a JSON file in the format:
```
[{
  <datasetName>: {
    'dataset_id': 'b3a7e84c-cf83-4c95-b6de-664986a14f5b',
    'snapshot_ids': ['a96d77bc-0809-42eb-b348-c0d4fe896ec4', ...]
  }
},
...]
```

Note: When run with the default values, this script will create datasets for the Data Repo UI tests
in the "integration 4" environment using a new billing profile.

### Adding a new dataset:
1. Create a new directory in "files".
2. Add a json file (named `dataset_schema.json`) that contains the JSON request to create the dataset.
3. Add a json file per dataset table (named `<table>.json`) containing the metadata rows to add to the dataset.
4. Upload the metadata files from #3 to a gcs bucket. Ensure that the user running the script, the user's proxy account,
and the Data Repo API service account have at least storage viewer access on the gcs bucket.
5. In the JSON file that lists which datasets to create (e.g. `datarepo_datasets.json`), define a new dataset where the
"schema" is the name of the new directory and the "upload_prefix" is the gs path to the metadata files.
