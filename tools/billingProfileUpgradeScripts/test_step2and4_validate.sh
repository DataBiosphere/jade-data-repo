#!/bin/bash
#
# Do this script after the upgrade release is installed, but before the upgrade is executed to make sure
# existing billing profiles can be used. It makes a dataset and deletes it.
# Do this script again after the upgrade is executed to make sure the billing profiles can still be used.
# NOTE: this is meant to be source'd not executed

# -- main --

 : ${DRAPI:?}
 : ${AUTH_TOKEN:?}
 : ${PROFILE_ID:?}

echo "PROFILE_ID=$PROFILE_ID"
echo "DRAPI=$DRAPI"

DATASET_BODY=$(cat <<EOF
{
  "name":        "canaryDataset",
  "description": "canary dataset",
  "defaultProfileId": "${PROFILE_ID}",
  "schema":      {
    "tables":        [
      {
        "name":    "participant",
        "columns": [
          {"name": "id", "datatype": "string"},
          {"name": "age", "datatype": "integer"}
        ]
      },
      {
        "name":    "sample",
        "columns": [
          {"name": "id", "datatype": "string"},
          {"name": "participant_id", "datatype": "string"},
          {"name": "date_collected", "datatype": "date"}
        ]
      }
    ],
    "relationships": [
      {
        "name": "participant_sample",
        "from": {"table": "participant", "column": "id"},
        "to":   {"table": "sample", "column": "participant_id"}
      }
    ],
    "assets":        [
      {
        "name":   "sample",
        "rootTable": "sample",
        "rootColumn": "id",
        "tables": [
          {"name": "sample", "columns": []},
          {"name": "participant", "columns": []}
        ],
        "follow": ["participant_sample"]
      }
    ]
  }
}
}
EOF
)

CREATE_JOB=$(echo $DATASET_BODY | curl -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH_TOKEN}" -d @- \
  ${DRAPI}/api/repository/v1/datasets)
echo "CREATE_JOB=$CREATE_JOB"
JOBID=$(echo $CREATE_JOB | jq -r ".id")
echo "JOBID=$JOBID"

RESULT=$(waitForComplete ${JOBID})
echo "RESULT=$RESULT"
DATASET_ID=$(echo $RESULT | jq -r ".id")
echo "Created dataset id ${DATASET_ID}"

DELETE_JOB=$(curl -X DELETE -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  ${DRAPI}/api/repository/v1/datasets/${DATASET_ID})
echo "DELETE_JOB=$DELETE_JOB"
JOBID=$(echo $DELETE_JOB | jq -r ".id")
echo "JOBID=$JOBID"

RESULT=$(waitForComplete ${JOBID})
echo "RESULT=$RESULT"

