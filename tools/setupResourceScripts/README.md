The `setup_tdr_resources.py` script can be used to easily create datasets and snapshots in the Data Repo.

### Usage:
1. If you need to set up a virtual python environment, you can run the following commands:
    * `python3 -m venv c:/path/to/myenv`
    * `source c:/path/to/myenv/bin/activate`
2. `cd jade-data-repo/tools/setupResourceScripts`
3. `pip3 install -r requirements.txt`
4. `gcloud auth login <user>`
5. `python3 setup_tdr_resources.py --help to see all flags used.`

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

### Set up data repo UI test datasets:

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


### Azure OMOP Dataset and Snapshot Builder Settings Setup
1. Prepare resources for creating a new azure billing profile (alternatively pass in an existing billing profile id with the --azure_profile_id flag)
   * If you do not already have a managed application created, follow instructions in jade-getting-started.md
   * Make note of the email account that set during managed app setup, you'll need to authenticate with this user
   * Make note of the "Application name" set during managed app setup. You'll pass this in with the --azure_application_name flag
   * Set the --billing_profile_file_name flag to the dev azure file: `--billing_profile_file_name='./files/azure_billing_profile_dev.json'`
2. Determine TDR instance you want to populate (i.e.local, BEE, etc.)
   * If running locally, set the --host flag to `http://localhost:8080`. Get TDR instance running locally by following instructions in jade-getting-started.md.
   * Otherwise, set the --host flag to the appropriate TDR instance.
3. If needed, set up your python instance:
   * `python3 -m venv c:/path/to/myenv`
   * `source c:/path/to/myenv/bin/activate`
4. Authenticate with the email account you set up during managed app setup:
   * `gcloud auth login <email>`
5. Run the python script
   * `cd jade-data-repo/tools/setupResourceScripts`
   * `pip3 install -r requirements.txt`
   * Run the set up script including all of the arguments, plus pointing to the azure OMOP dataset file:
   * `python3 setup_tdr_resources.py --host <datarepo_url> --datasets='./files/azure_omop_dataset.json' --azure_application_name <azure_application_name> --billing_profile_file_name './files/azure_billing_profile_dev.json'`
   * If you have a billing account already created, you can use the `--azure_profile_id` flag to pass in the billing profile id, then you no longer need to pass in some of the other flags:
   * `python3 setup_tdr_resources.py --host <datarepo_url> --datasets='./files/azure_omop_dataset.json' --azure_profile_id <profile_id>`

### GCP OMOP Dataset and Snapshot Builder Settings Setup
1. Determine TDR instance you want to populate (i.e.local, BEE, etc.)
  * If running locally, set the --host flag to `http://localhost:8080`. Get TDR instance running locally by following instructions in jade-getting-started.md.
  * Otherwise, set the --host flag to the appropriate TDR instance.
2. If needed, set up your python instance:
  * `python3 -m venv c:/path/to/myenv`
  * `source c:/path/to/myenv/bin/activate`
3. Authenticate with the email account that has access to the GCP billing account `00708C-45D19D-27AAFA`:
  * `gcloud auth login <email>`
4. Run the python script
  * `cd jade-data-repo/tools/setupResourceScripts`
  * `pip3 install -r requirements.txt`
  * Run the setup script including all the arguments, plus pointing to the GCP OMOP dataset file:
  * `python3 setup_tdr_resources.py --host <datarepo_url> --datasets='./files/gcp_omop_dataset.json'`
  * If you have a billing account already created, you can use the `--gcp_profile_id` flag to pass in the billing profile id.
