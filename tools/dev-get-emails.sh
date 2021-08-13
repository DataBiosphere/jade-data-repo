#!/bin/bash
# Returns a csv of all steward emails for all datasets in the dev environment

set -eu

read -rp "Enter your dev email: " DEV_EMAIL

ORIG_EMAIL=$(gcloud config get-value account 2>/dev/null)
gcloud auth login "${DEV_EMAIL}" 2>/dev/null
TOKEN=$(gcloud auth print-access-token)
gcloud auth login "${ORIG_EMAIL}" 2>/dev/null

dataset_list() {
    curl -s -X GET "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/datasets?direction=desc&limit=10000&offset=0&sort=created_date" \
        -H "accept: application/json" \
        -H "authorization: Bearer ${TOKEN}" \
            | jq -r '.items[] | "\(.id) \(.name)"'
}

dataset_policy() {
    curl -s -X GET "https://sam.dsde-dev.broadinstitute.org/api/resources/v2/dataset/${1}/policies" \
        -H "accept: application/json" \
        -H "authorization: Bearer ${TOKEN}"
}

dataset_emails() {
    IFS=$'\n'
    ALL_EMAILS=()
    for ROW in $(dataset_list) ; do
        DATASET_ID=$(echo "${ROW}" | cut -d' ' -f1)
        DATASET_NAME=$(echo "${ROW}" | cut -d' ' -f2)
        DATASET_EMAILS=$(dataset_policy "${DATASET_ID}" \
            | jq --arg NAME "${DATASET_NAME}" '.[] | select(.policyName == "steward") | .policy | {memberEmails} | . + {name: $NAME}')
        ALL_EMAILS+=("${DATASET_EMAILS}")
    done
    echo "${ALL_EMAILS[@]}" | jq -s '.' > emails.json
}

json_to_csv_emails() {
    jq -r '.[] | [.name] + [.memberEmails[]] | @csv' emails.json > emails.csv
}

dataset_emails
json_to_csv_emails
