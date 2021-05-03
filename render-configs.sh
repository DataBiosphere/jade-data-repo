#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
    vault read -format=json secret/dsde/datarepo/dev/sa-key.json | \
    jq .data | tee /tmp/jade-dev-account.json | \
    jq -r .private_key > /tmp/jade-dev-account.pem

#new
BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/dev/dev/buffer/client-sa
#integration
#BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH=/tmp/buffer-client-sa-account.json

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
    vault read -format=json ${BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH} | \
    jq -r .data.key | base64 -d > ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}
