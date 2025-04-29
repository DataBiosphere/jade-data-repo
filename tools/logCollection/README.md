The `collect-logs.py` script can be used to transform logs from TDR GCP logging into a tabular format
with more details.

### Usage:
1. If you need to set up a virtual python environment, you can run the following commands:
  * `python3 -m venv c:/path/to/myenv`
  * `source c:/path/to/myenv/bin/activate`
2. `cd jade-data-repo/tools/logCollection`
3. `pip3 install -r requirements.txt --upgrade`
4. `gcloud auth login <user>@firecloud.org` (Need firecloud account to access production user data)
5. Must collect logs from gcloud console, download them in JSON format and put the JSON file in the
`logCollection/rawLogs` folder. This file is what needs to be referenced in the `--raw-logs` argument
for the script.
5. `python3 collect-logs.py --help to see all flags used.`
6. Formatted logs will output in the `logCollection/output` folder

### Notes about output:
There are a couple of fields that need to be manually populated based on the dataset/snapshot
that we are logging:
* `nih_ico`
* `cadr_name`

There are a couple of hardcoded fields:
* `user_id_provider` is set to `Terra`
* `app` is set to `TDR`

Not relevant fields:
* `eRA_commons_id ` - until RAS is supported in production, this is empty. It will require work to populate this field.



