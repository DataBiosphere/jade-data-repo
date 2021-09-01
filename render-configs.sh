#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

vault read -field=applicationsecret secret/dsde/datarepo/dev/helm-azure \
    > /tmp/jade-dev-azure.key

BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH=/tmp/buffer-client-sa-account.json

vault read -field=key ${BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH} \
    | base64 -d > ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}
