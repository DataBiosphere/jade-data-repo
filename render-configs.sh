#!/bin/bash

# This script pulls the needed secrets from vault and sets the values as environment variables
# If you want these values to show up in intellij test/run profiles, you need to restart intellij after running this script

# If you want to run Azure Integration tests locally, you need to point to the "integration" environment
ENV=${1:-dev}
LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

vault read -field=tenant-id secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-tenant-id.key

vault read -field=client-id secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-client-id.key

vault read -field=client-secret secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-azure.key

vault read -field=synapse-sql-admin-user secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-synapse-admin-user.key

vault read -field=synapse-sql-admin-password secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-synapse-admin-password.key

vault read -field=synapse-encryption-key secret/dsde/datarepo/${ENV}/azure-application-secrets \
    > /tmp/jade-${ENV}-synapse-encryption-key.key

BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH=/tmp/buffer-client-sa-account.json

vault read -field=key ${BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH} \
    | base64 -d > ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}

export GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
export AZURE_CREDENTIALS_APPLICATIONID=$(cat /tmp/jade-${ENV}-client-id.key)
export AZURE_CREDENTIALS_SECRET=$(cat /tmp/jade-${ENV}-azure.key)
export AZURE_SYNAPSE_SQLADMINUSER=$(cat /tmp/jade-${ENV}-synapse-admin-user.key)
export AZURE_SYNAPSE_SQLADMINPASSWORD=$(cat /tmp/jade-${ENV}-synapse-admin-password.key)
export AZURE_CREDENTIALS_HOMETENANTID=$(cat /tmp/jade-${ENV}-tenant-id.key)
export AZURE_SYNAPSE_ENCRYPTIONKEY=$(cat /tmp/jade-${ENV}-synapse-encryption-key.key)
