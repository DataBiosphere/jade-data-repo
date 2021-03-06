#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
    vault read -format=json secret/dsde/datarepo/dev/sa-key.json | \
    jq .data | tee /tmp/jade-dev-account.json | \
    jq -r .private_key > /tmp/jade-dev-account.pem

