#!/bin/bash
BRANCH="ch-data-repo-alpha-integration-tests"
TOKEN=${1:-$GH_TOKEN}
REPO="DataBiosphere/jade-data-repo"
WORKFLOW="alpha-integration-tests.yaml"

curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token ${TOKEN}" \
    --request POST \
    --data '{"ref": "'"${BRANCH}"'"}' \
    https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches
