#!/bin/bash
set -e

usage () {
    cat <<HELP_USAGE

    $0  [-u <url>]

    -u  pass a url in to hit, like http://localhost:8080
    -h  print this help message
HELP_USAGE
}

while getopts ":u:h" opt; do
    case $opt in
        u)
            echo "overriding host value" >&2
            HOST=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "invalid option: -$OPTARG" >&2
            usage
            exit 1
            ;;
        :)
            echo "option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done

if [ -z ${HOST+x} ]; then
    : ${ENVIRONMENT:?}
    : ${SUFFIX:?}
    HOST="https://jade-${SUFFIX}.datarepo-${ENVIRONMENT}.broadinstitute.org"
    echo "hitting $HOST"
fi

# switch to the steward account to get the right access token then switch back
: ${STEWARD_ACCT:?}
SAVED_ACCT=$(gcloud config get-value account)
gcloud config set account $STEWARD_ACCT
ACCESS_TOKEN=$(gcloud auth print-access-token)
gcloud config set account $SAVED_ACCT

# this will be the path to the tools directory
WD=$( dirname "${BASH_SOURCE[0]}" )

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
STUDY_NAME="ingest_test_${STAMP}"
STUDY_ID=$(cat ${WD}/../src/test/resources/ingest-test-study.json \
    | jq ".defaultProfileId = ${PROFILE_ID} | .name = \"${STUDY_NAME}\"" \
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

sleep 5

cat ${WD}/../src/test/resources/ingest-test-dataset.json \
    | jq ".name = \"ingest_test_ds_${STAMP}\" | .contents[0].source.studyName = \"${STUDY_NAME}\"" \
    | jq ".profileId = ${PROFILE_ID}" \
    | curl -v -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' \
        --header "Authorization: Bearer ${ACCESS_TOKEN}" \
        -d @- "${HOST}/api/repository/v1/datasets"
