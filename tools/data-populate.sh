#!/bin/bash
set -e

: ${ENVIRONMENT:?}
: ${SUFFIX:?}
: ${STEWARD_ACCT:?}

SAVED_ACCT=$(gcloud config get-value account)
# switch to the steward account to get the right access token then switch back
gcloud config set account $STEWARD_ACCT
ACCESS_TOKEN=$(gcloud auth print-access-token)
gcloud config set account $SAVED_ACCT

HOST="https://jade-${SUFFIX}.datarepo-${ENVIRONMENT}.broadinstitute.org"
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
STAMP=$(date +"%m_%d_%H_%M")
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
  "path": "gs://jade-testdata/ingest-test/ingest-test-__replace__.json",
  "table": "__replace__"
}
EOF
)

tables=(file sample participant)
for table in "${tables[@]}"
do
    echo $INGEST_PAYLOAD \
        | sed "s/__replace__/${table}/g" \
        | curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' \
            --header "Authorization: Bearer ${ACCESS_TOKEN}" \
            -d @- "${HOST}/api/repository/v1/studies/${STUDY_ID}/ingest"
done
