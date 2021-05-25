#!/bin/bash
# a script to copy a dataset from one deployment to another

set -eu

### INPUTS ###
DATASET_UUID_TO_COPY="00000000-0000-0000-0000-000000000000"
AUTH_TOKEN="authorization: Bearer ya29"

SRC_URL="https://jade.datarepo-dev.broadinstitute.org"
DST_URL="https://jade-fb.datarepo-dev.broadinstitute.org/"

### utility functions ###
token_check() {
    RESULT=$(curl -s -X GET "${SRC_URL}/api/resources/v1/profiles" -H "accept: application/json" -H "${AUTH_TOKEN}" -w "%{http_code}" -o /dev/null)
    if [[ ${RESULT} =~ "401" ]]; then
        echo "Error: AUTH_TOKEN is invalid or expired"
        exit 1
    fi
}

uuid_lc() {
    uuidgen | tr '[:upper:]' '[:lower:]'
}

post_url() {
    curl -s -X POST "${1}" -H  "accept: application/json" -H  "Content-Type: application/json" -H "${AUTH_TOKEN}" -d "${2}"
}

get_url() {
    curl -s -X GET  "${1}" -H  "accept: application/json" -H  "Content-Type: application/json" -H "${AUTH_TOKEN}"
}

CURRENT_DATE=$(date +%Y%m%d)

### PART 1: create profile ###
profile_create() {
    echo "1: creating new profile"

    PROFILE_URL="${DST_URL}/api/resources/v1/profiles"

    PROFILE_UUID=$(uuid_lc)
    PROFILE_NAME="profile-search-api-${CURRENT_DATE}-${PROFILE_UUID:0:8}"

    PROFILE_JSON=$(cat << EOF
{
  "biller": "direct",
  "billingAccountId": "00708C-45D19D-27AAFA",
  "description": "profile created by datasetcopy.sh",
  "id": "${PROFILE_UUID}",
  "profileName": "${PROFILE_NAME}"
}
EOF
)

    post_url "${PROFILE_URL}" "${PROFILE_JSON}"

    printf "\n1: created profile %s\n" "${PROFILE_UUID}"
}

### PART 2: copy dataset schema ###
dataset_copy_schema() {
    echo "2: copying dataset schema"

    DATASET_URL="${SRC_URL}/api/repository/v1/datasets/${DATASET_UUID_TO_COPY}?include=SCHEMA&include=ACCESS_INFORMATION"

    DATASET_JSON=$(get_url "${DATASET_URL}")
    DATASET_NAME=$(echo "${DATASET_JSON}" | jq -r '.name')
    DATASET_SCHEMA=$(echo "${DATASET_JSON}" \
        | jq 'del(.id, .dataProject, .defaultSnapshotId, .createdDate, .storage, .accessInformation) | . + { "cloudPlatform": "gcp", "region": "US" } | .defaultProfileId = "'"${PROFILE_UUID}"'"' \
        | sed 's/"datatype": "fileref"/"datatype": "string"/')
    DATASET_TABLES=$(echo "${DATASET_JSON}" | jq -r '.accessInformation.bigQuery.tables[].qualifiedName')
    
    echo "${DATASET_SCHEMA}" > schema.json

    echo "2: copied schema for dataset ${DATASET_UUID_TO_COPY}"
}

### part 3: extract dataset data ###
dataset_extract_data() {
    echo "3: saving dataset into temporary directory"

    DATASET_TMP_DIR=$(mktemp -d)
    for DATASET_TABLE in ${DATASET_TABLES} ; do
        DATASET_TMP_JSON0="${DATASET_TMP_DIR}/${DATASET_TABLE}.json"
        DATASET_TMP_JSONL="${DATASET_TMP_DIR}/${DATASET_TABLE}.jsonl"
        bq query --nouse_legacy_sql --format=json 'SELECT * FROM `'"${DATASET_TABLE}"'`' > "${DATASET_TMP_JSON0}"
        jq -c -r '.[]' < "${DATASET_TMP_JSON0}" > "${DATASET_TMP_JSONL}"
    done

    echo "3: saved dataset data into ${DATASET_TMP_DIR}"
}

### part 4: copy dataset data in bucket ###
gcs_copy_dataset_data() {
    echo "4: copying data into gcs"

    DATASET_GCS_DIR=$(basename "${DATASET_TMP_DIR}" | tr '[:upper:]' '[:lower:]' | sed 's/\./-/')
    DATASET_GCS_DIR="gs://${DATASET_GCS_DIR}-${CURRENT_DATE}"

    gsutil mb "${DATASET_GCS_DIR}"
    gsutil cp "${DATASET_TMP_DIR}/*.jsonl" "${DATASET_GCS_DIR}"

    echo "4: copied data into bucket ${DATASET_GCS_DIR}"
}

### part 5a: create dataset schema ###
dataset_create_schema() {
    echo "5: creating schema"

    DATASET_URL="${DST_URL}/api/repository/v1/datasets"

    post_url "${DATASET_URL}" "${DATASET_SCHEMA}"
    
    printf "\n"
    Z=0
    while true ; do
        DATASET_NEW_UUID=$(get_url "${DATASET_URL}" | jq -r '.items[] | select(.name=="'"${DATASET_NAME}"'") | .id')

        if [ -n "${DATASET_NEW_UUID}" ]; then
            echo "5: created schema for NEW dataset ${DATASET_NEW_UUID}"
            break
        elif [ "${Z}" -ge 6 ]; then
            echo "Error: dataset not found after multiple retries"
            exit 1
        fi

        echo "5: could not find dataset, sleeping"
        sleep 10

        Z=$((Z+1))
    done
}

### part 5b: ingest the files ###
gcs_ingest_dataset() {
    echo "5: ingesting dataset tables from gcs"

    DATASET_URL="${DST_URL}/api/repository/v1/datasets/${DATASET_NEW_UUID}/ingest"

    GCS_FILES=$(gsutil ls "${DATASET_GCS_DIR}")
    LOAD_TAG_UUID=$(uuid_lc)

    for GCS_FILE in ${GCS_FILES} ; do
        TABLE_NAME=$(echo "${GCS_FILE}" | sed -r 's/(.+\.)(\w+)(\.jsonl)/\2/')
        TABLE_JSON=$(cat << EOF
{
    "format": "json",
    "ignore_unknown_values": true,
    "load_tag": "${LOAD_TAG_UUID}",
    "max_bad_records": 0,
    "path": "${GCS_FILE}",
    "table": "${TABLE_NAME}"
}
EOF
)

        post_url "${DATASET_URL}" "${TABLE_JSON}"

        printf "\n"
    done

    echo "5: ingested dataset tables from gcs"
}

### part 6: finish up ###
final_msg() {
    echo "!!! COMPLETED DATASET COPY, CHECK JOBS AND LOGS FOR FAILURES DURING INGESTION !!!"
}

### Run all the functions ###
exec_main() {
    token_check
    profile_create
    dataset_copy_schema
    dataset_extract_data
    gcs_copy_dataset_data
    dataset_create_schema
    gcs_ingest_dataset
    final_msg
}

exec_main
