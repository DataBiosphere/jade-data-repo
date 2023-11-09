#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

vault read -field=tenant-id secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-tenant-id.key

vault read -field=client-id secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-client-id.key

vault read -field=client-secret secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-azure.key

vault read -field=synapse-sql-admin-user secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-synapse-admin-user.key

vault read -field=synapse-sql-admin-password secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-synapse-admin-password.key

vault read -field=synapse-encryption-key secret/dsde/datarepo/dev/azure-application-secrets \
    > /tmp/jade-dev-synapse-encryption-key.key

vault read -field=basic_auth_read_only_username secret/dsp/pact-broker/users/read-only \
    > /tmp/pact-ro-username.key

vault read -field=basic_auth_read_only_password secret/dsp/pact-broker/users/read-only \
    > /tmp/pact-ro-password.key

BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH=/tmp/buffer-client-sa-account.json

vault read -field=key ${BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH} \
    | base64 -d > ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}
