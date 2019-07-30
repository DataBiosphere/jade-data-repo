#!/bin/bash
set -e

ACCESS_TOKEN=$(gcloud auth print-access-token)
HOST=https://jade-jh.datarepo-dev.broadinstitute.org
#HOST=http://localhost:8080

# use the first profile id if it is there
PROFILE_ID=$(curl --header 'Accept: application/json' --header "Authorization: Bearer ${ACCESS_TOKEN}" \
    "${HOST}/api/resources/v1/profiles" \
    | jq .items[0].id)

# if not, make a new one
if [ "$PROFILE_ID" == "null" ]; then
    PROFILE_ID=$(curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' \
        --header "Authorization: Bearer ${ACCESS_TOKEN}" \
        -d '{ "biller": "direct", "billingAccountId": "00708C-45D19D-27AAFA", "profileName": "core" }' \
        "${HOST}/api/resources/v1/profiles" \
        | jq .id)
fi

# create the encode study
STAMP=$(date +"%Y_%m_%d_%H_%M_%S")
STUDY_ID=$(cat ../src/test/resources/ingest-test-study.json \
    | jq ".defaultProfileId = ${PROFILE_ID} | .name = \"ingest_test_${STAMP}\"" \
    | curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' \
        --header "Authorization: Bearer ${ACCESS_TOKEN}" \
        -d @- "${HOST}/api/repository/v1/studies" \
    | jq -r .id)

# ingest file data into it
INGEST_PAYLOAD=$(cat <<EOF
{
  "format": "json",
  "ignore_unknown_values": false,
  "load_tag": "data-populate.sh",
  "max_bad_records": 0,
  "path": "gs://jade-testdata/ingest-test/ingest-test-file.json",
  "table": "file"
}
EOF
)

curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' \
    --header "Authorization: Bearer ${ACCESS_TOKEN}" \
    -d "$INGEST_PAYLOAD" \
    "${HOST}/api/repository/v1/studies/${STUDY_ID}/ingest"
