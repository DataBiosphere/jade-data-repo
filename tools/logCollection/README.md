The `collect-logs.py` script can be used to transform logs from TDR GCP logging into a tabular format
with more details.

### Usage:
**NOTE:** Must first collect logs from gcloud console, download them in JSON format and put the JSON file in the
`logCollection/rawLogs` folder. This file is what needs to be referenced in the `--raw_logs_location` argument
for the script.

1. If you need to set up a virtual python environment, you can run the following commands:
  * `python3 -m venv c:/path/to/myenv`
  * `source c:/path/to/myenv/bin/activate`
2. `cd jade-data-repo/tools/logCollection`
3. `pip3 install -r requirements.txt --upgrade`
4. `gcloud auth application-default login <user>@firecloud.org` (Need firecloud account to access production user data)
5. Run the command pointed at the raw log location:
```
python3 collect-logs.py --raw_logs_location=<path to raw log file>
```
Or the help command to see all options:
`python3 collect-logs.py --help to see all flags used.`
6. Formatted logs will output in the `logCollection/output` folder

#### Example:
`python3 collect_logs.py --raw_logs_location=sample_tdr_logs.json`

#### Testing:
Run all tests with the following command:
`python3 -m unittest src/tests/*`

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

Notes:
* `associated_study` - This will populate with a list of PHS IDs and/or DUOS ids if available

