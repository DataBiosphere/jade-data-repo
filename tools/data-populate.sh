#!/bin/bash

set -ex

# first argument must be a server address with a protocol
: ${1?"server address required"}

ACCESS_TOKEN=$( gcloud beta auth application-default print-access-token )

WD=$( dirname "${BASH_SOURCE[0]}" )
BASE="${1}/api/repository/v1"

# create a few studies
curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/study-minimal.json"

curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/encode-study.json"

curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/it-study-omop.json"

curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/dataset-test-study.json"

curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/study-create-test.json"

curl "${BASE}/studies" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/ingest-test-study.json"

# populate tables
bq load --source_format=CSV --skip_leading_rows=1 datarepo_Minimal.participant \
    "${WD}/../src/test/resources/study-minimal-participant.csv"

bq load --source_format=CSV --skip_leading_rows=1 datarepo_Minimal.sample \
    "${WD}/../src/test/resources/study-minimal-sample.csv"

# create a dataset
curl "${BASE}/datasets" -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${WD}/../src/test/resources/study-minimal-dataset.json"
