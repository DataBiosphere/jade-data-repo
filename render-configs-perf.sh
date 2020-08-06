#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
    vault read -format=json secret/dsde/datarepo/perf/datarepo-api-sa  | jq -r .data.key | base64 --decode \
    | tee /tmp/jade-perf-account.json | \
    jq -r .private_key > /tmp/jade-perf-account.pem
