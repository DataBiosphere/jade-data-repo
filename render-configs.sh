#!/bin/bash
### How to call this script - we need to use the "source" command in order to export the environment variables
# source ./render-configs.sh
# source ./render-configs.sh integration

### About this script
# This script pulls the needed secrets from vault and sets the values as environment variables
# If you want these values to show up in intellij test/run profiles, you need to restart intellij after running this script

# ========================
# Azure Credentials
# ========================
# If you want to run Azure Integration tests locally, you need to point to the "integration" environment
# Options: 'dev', 'integration'
AZURE_ENV=${1:-dev}

if [[ "${AZURE_ENV}" == "dev" ]]; then
    AZURE_SYNAPSE_WORKSPACENAME=tdr-synapse-us-east-ondemand.sql.azuresynapse.net
elif [[ "${AZURE_ENV}" == "integration" ]]; then
    AZURE_SYNAPSE_WORKSPACENAME=tdr-snps-int-east-us-ondemand.sql.azuresynapse.net
else
    echo "Invalid Azure environment: $AZURE_ENV"
    exit 1
fi
export AZURE_SYNAPSE_WORKSPACENAME


vault read -field=tenant-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-$AZURE_ENV-tenant-id.key"
AZURE_CREDENTIALS_HOMETENANTID=$(cat "/tmp/jade-$AZURE_ENV-tenant-id.key")
export AZURE_CREDENTIALS_HOMETENANTID


vault read -field=client-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-${AZURE_ENV}-client-id.key"
AZURE_CREDENTIALS_APPLICATIONID=$(cat "/tmp/jade-${AZURE_ENV}-client-id.key")
export AZURE_CREDENTIALS_APPLICATIONID


vault read -field=client-secret secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-$AZURE_ENV-azure.key"
AZURE_CREDENTIALS_SECRET=$(cat "/tmp/jade-$AZURE_ENV-azure.key")
export AZURE_CREDENTIALS_SECRET


vault read -field=synapse-sql-admin-user secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-$AZURE_ENV-synapse-admin-user.key"
AZURE_SYNAPSE_SQLADMINUSER=$(cat "/tmp/jade-$AZURE_ENV-synapse-admin-user.key")
export AZURE_SYNAPSE_SQLADMINUSER


vault read -field=synapse-sql-admin-password secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-$AZURE_ENV-synapse-admin-password.key"
AZURE_SYNAPSE_SQLADMINPASSWORD=$(cat "/tmp/jade-$AZURE_ENV-synapse-admin-password.key")
export AZURE_SYNAPSE_SQLADMINPASSWORD


vault read -field=synapse-encryption-key secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-$AZURE_ENV-synapse-encryption-key.key"
AZURE_SYNAPSE_ENCRYPTIONKEY=$(cat "/tmp/jade-$AZURE_ENV-synapse-encryption-key.key")
export AZURE_SYNAPSE_ENCRYPTIONKEY

# ========================
# Google Credentials
# ========================
vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
export GOOGLE_APPLICATION_CREDENTIALS

# ========================
# Resource Buffer Service
# ========================
# Other option: dev - this will allow for projects to persist for longer than 1 day
RBS_ENV=${2:-tools}
if [[ "${RBS_ENV}" == "tools" ]]; then
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
    RBS_POOLID=datarepo_v1
    RBS_INSTANCEURL=https://buffer.tools.integ.envs.broadinstitute.org
elif [[ "${AZURE_ENV}" == "dev" ]]; then
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/dev/dev/buffer/client-sa
    RBS_POOLID=datarepo_v3
    RBS_INSTANCEURL=https://buffer.dsde-dev.broadinstitute.org
else
    echo "Invalid RBS environment: $RBS_ENV - only 'tools' and 'dev' are supported."
    exit 1
fi

RBS_CLIENTCREDENTIALFILEPATH=/tmp/buffer-client-sa-account.json
RBS_ENABLED=true
RBS_CLIENTCREDENTIALFILEPATH=/tmp/buffer-client-sa-account.json

vault read -field=key "$BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH" \
    | base64 -d > "$RBS_CLIENTCREDENTIALFILEPATH"

export RBS_ENABLED
export RBS_POOLID
export RBS_INSTANCEURL
export RBS_CLIENTCREDENTIALFILEPATH
