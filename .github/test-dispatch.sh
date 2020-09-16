#!/bin/bash
BRANCH="sh-DR-1116-perf-action"
TOKEN=$(cat $HOME/.gh_token)
REPO="DataBiosphere/jade-data-repo"
WORKFLOW="test-runner-on-perf.yml"

curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token ${TOKEN}" \
    --request POST \
    --data '{"ref": "'"${BRANCH}"'"}' \
    https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches
