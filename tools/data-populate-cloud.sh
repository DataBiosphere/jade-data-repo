#!/bin/bash
#
# Populates a Terra Data Repo instance with a dataset, test data, and a snapshot.
# You MUST have curl and jq installed to run this script.
#
# See usage section below for more details. All arguments are optional.
#

set -eu
set -o pipefail

usage() {
    cat <<EOF
Usage: $0 [OPTION]...
Populate a Terra Data Repo instance with test data

  --url URL             Base URL of the Terra Data Repo API
  --cloud CLOUD         Cloud platform to use for data ingest
  --profile PROFILE     Use a specific billing profile for data ingest
  --help                Display this help and exit
EOF
    exit 0
}

error() {
    echo "ERROR: $1" >&2
    exit 1
}

# default values that may be overridden by command line arguments
URL="http://localhost:8080"
CLOUD="gcp"
PROFILE=""

parse_cli_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --cloud)
                CLOUD=$2
                shift 2
                ;;
            --profile)
                PROFILE=$2
                shift 2
                ;;
            --url)
                URL=$2
                shift 2
                ;;
            --help)
                usage
                ;;
            *)
                error "Unknown option: $1. Try --help to see a list of all options."
                ;;
        esac
    done
}

curl_get() {
    curl --silent --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" "$1"
}

curl_post() {
    curl --silent --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" --data "$1" "$2"
}

get_billing_profile() {
    if [ -z "$PROFILE" ]; then
        PROFILE_LIST=$(curl_get "$URL/api/resources/v1/profiles?limit=1000")
        PROFILE_ID=$(echo "$PROFILE_LIST" | jq -r --arg cloud "$CLOUD" '[ .items.[] | select(.cloudPlatform == $cloud) ].[0].id')
    else
        PROFILE_LIST=$(curl_get "$URL/api/resources/v1/profiles/$PROFILE")
        PROFILE_ID=$(echo "$PROFILE_LIST" | jq -r '.id')
        CLOUD=$(echo "$PROFILE_LIST" | jq -r '.cloudPlatform')
    fi
    if [ "$PROFILE_ID" == "null" ]; then
        if [ -z "$PROFILE" ]; then
            error "No billing profile found for cloud platform '$CLOUD'"
        else
            error "Billing profile with ID '$PROFILE' not found"
        fi
    else
        echo "Using billing profile with ID '$PROFILE_ID'"
    fi
}

post_dataset() {
    USER_INITIALS=$(gcloud config list account --format "value(core.account)" | head -c2)
    CUR_DATE=$(date +%Y%m%d)
    CUR_SECS=$(date +%s)
    DATASET_NAME="${USER_INITIALS}_test_dataset_${CUR_DATE}_${CUR_SECS}"
    DATASET_HEADER=$(cat <<EOF
{
    "name": "$DATASET_NAME",
    "description": "Created by $USER_INITIALS on $CUR_DATE using $0",
    "defaultProfileId": "$PROFILE_ID",
    "cloudPlatform": "$CLOUD"
}
EOF
)
    DATASET_SCHEMA=$(jq '.schema | { schema: . }' ../src/test/resources/dataset-minimal.json)
    DATASET_REQUEST=$(echo "$DATASET_HEADER $DATASET_SCHEMA" | jq -s add)
    DATASET_RESPONSE=$(curl_post "$DATASET_REQUEST" "$URL/api/repository/v1/datasets")
    DATASET_JOB=$(echo "$DATASET_RESPONSE" | jq -r '.id')
    while true; do
        JOB_STATUS=$(curl_get "$URL/api/repository/v1/jobs/$DATASET_JOB/result")
        if [ "$JOB_STATUS" != "" ]; then
            DATASET_ID=$(echo "$JOB_STATUS" | jq -r '.id')
            echo "Dataset created with ID '$DATASET_ID'"
            break
        fi
        sleep 5
    done
}

post_ingest() {
    TABLE_LIST=("participant" "sample")
    for TABLE in "${TABLE_LIST[@]}"; do
        INGEST_HEADER=$(cat <<EOF
{
    "load_tag": "${TABLE}_${CUR_DATE}_${CUR_SECS}",
    "format": "array",
    "table": "$TABLE",
    "updateStrategy": "append"
}
EOF
)
        TABLE_DATA=$(jq -s '. | { records: . }' ../src/test/resources/dataset-minimal-"$TABLE".json)
        INGEST_REQUEST=$(echo "$INGEST_HEADER $TABLE_DATA" | jq -s add)
        INGEST_RESPONSE=$(curl_post "$INGEST_REQUEST" "$URL/api/repository/v1/datasets/$DATASET_ID/ingest")
        INGEST_JOB=$(echo "$INGEST_RESPONSE" | jq -r '.id')
        while true; do
            JOB_STATUS=$(curl_get "$URL/api/repository/v1/jobs/$INGEST_JOB/result")
            if [ "$JOB_STATUS" != "" ]; then
                echo "Ingest job with ID '$INGEST_JOB' for table '$TABLE' completed"
                break
            fi
            sleep 5
        done
    done
}

post_snapshot() {
    SNAPSHOT_NAME="${USER_INITIALS}_test_snapshot_${CUR_DATE}_${CUR_SECS}"
    SNAPSHOT_REQUEST=$(cat <<EOF
{
    "name": "$SNAPSHOT_NAME",
    "description": "Created by $USER_INITIALS on $CUR_DATE using $0",
    "profileId": "$PROFILE_ID",
    "contents": [{
        "datasetName": "$DATASET_NAME",
        "mode": "byFullView"
    }]
}
EOF
)
    SNAPSHOT_RESPONSE=$(curl_post "$SNAPSHOT_REQUEST" "$URL/api/repository/v1/snapshots")
    SNAPSHOT_JOB=$(echo "$SNAPSHOT_RESPONSE" | jq -r '.id')
    while true; do
        JOB_STATUS=$(curl_get "$URL/api/repository/v1/jobs/$SNAPSHOT_JOB/result")
        if [ "$JOB_STATUS" != "" ]; then
            SNAPSHOT_ID=$(echo "$JOB_STATUS" | jq -r '.id')
            echo "Snapshot created with ID '$SNAPSHOT_ID'"
            break
        fi
        sleep 5
    done
}

AUTH_TOKEN=$(gcloud auth print-access-token)

parse_cli_args "$@"
get_billing_profile
post_dataset
post_ingest
post_snapshot
