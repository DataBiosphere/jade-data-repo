#!/bin/bash
BRANCH="sh-DR-1116-perf-action"
TOKEN=${1:-$GH_TOKEN}
REPO="DataBiosphere/jade-data-repo"
WORKFLOW="perf-tests.yaml"

curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token ${TOKEN}" \
    --request POST \
    --data '{"ref": "'"${BRANCH}"'"}' \
    https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches