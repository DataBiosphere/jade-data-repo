#!/bin/bash
### How to call this script
# To write secrets to tmp files:
# ./render-configs.sh (defaults to dev azure, tools RBS)

# If you want to export the related environment variables, use the "source" command:
# source ./render-configs.sh (defaults to dev azure, tools RBS)

# There are three optional arguments:
# source ./render-configs.sh (Azure Synapse: -a dev|integration) (RBS: -r tools|dev) (Print out env variables: -p)
# e.g.: source ./render-configs.sh -a dev -r tools -p
# This would set azure synapse to dev, RBS to tools, and print out the environment variables

# If you're running Azure Integration Tests you should use the following settings:
# source ./render-configs.sh -a integration -r tools -p
# Add the printed environment variables to your bash profile or add them to the Intellij test profile.

# If you want a set up locally, you can use the following settings:
# source ./render-configs.sh -a dev -r dev
# ./gradlew bootRun

### About this script
# This script pulls the needed secrets from vault and sets the values as environment variables
# If you want these values to show up in intellij test/run profiles, you need to restart intellij after running this script

AZURE_ENV=dev
RBS_ENV=tools
PRINT_ENV_VARS=n

while getopts ":a:r:p" option; do
  case $option in
    a)
      AZURE_ENV=$OPTARG
      ;;
    r)
      RBS_ENV=$OPTARG
      ;;
    p)
      PRINT_ENV_VARS=y
      ;;
    *)
      echo "Usage: $0 [-a (dev|integration)] [-r (tools|dev)] [-p]"
      exit 1
      ;;
  esac
done

# ========================
# Azure Credentials
# ========================
# If you want to run Azure Integration tests locally, you need to point to the "integration" environment
# Options: 'dev', 'integration'

if [[ "${AZURE_ENV}" == "dev" ]]; then
    AZURE_SYNAPSE_WORKSPACENAME=tdr-synapse-east-us-ondemand.sql.azuresynapse.net
elif [[ "${AZURE_ENV}" == "integration" ]]; then
    AZURE_SYNAPSE_WORKSPACENAME=tdr-snps-int-east-us-ondemand.sql.azuresynapse.net
else
    echo "Invalid Azure environment: $AZURE_ENV"
    exit 1
fi
export AZURE_SYNAPSE_WORKSPACENAME


vault read -field=tenant-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-azure-tenant-id.key"
AZURE_CREDENTIALS_HOMETENANTID=$(cat "/tmp/tdr-azure-tenant-id.key")
export AZURE_CREDENTIALS_HOMETENANTID


vault read -field=client-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-azure-client-id.key"
AZURE_CREDENTIALS_APPLICATIONID=$(cat "/tmp/tdr-azure-client-id.key")
export AZURE_CREDENTIALS_APPLICATIONID


vault read -field=client-secret secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-azure-client-secret.key"
AZURE_CREDENTIALS_SECRET=$(cat "/tmp/tdr-azure-client-secret.key")
export AZURE_CREDENTIALS_SECRET


vault read -field=synapse-sql-admin-user secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-synapse-admin-user.key"
AZURE_SYNAPSE_SQLADMINUSER=$(cat "/tmp/tdr-synapse-admin-user.key")
export AZURE_SYNAPSE_SQLADMINUSER


vault read -field=synapse-sql-admin-password secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-synapse-admin-password.key"
AZURE_SYNAPSE_SQLADMINPASSWORD=$(cat "/tmp/tdr-synapse-admin-password.key")
export AZURE_SYNAPSE_SQLADMINPASSWORD


vault read -field=synapse-encryption-key secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/tdr-synapse-encryption-key.key"
AZURE_SYNAPSE_ENCRYPTIONKEY=$(cat "/tmp/tdr-synapse-encryption-key.key")
export AZURE_SYNAPSE_ENCRYPTIONKEY

# ========================
# Google Credentials
# ========================
vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
export GOOGLE_APPLICATION_CREDENTIALS
GOOGLE_SA_CERT=/tmp/jade-dev-account.pem
export GOOGLE_SA_CERT

# ========================
# Resource Buffer Service
# ========================
# By default, RBS will use the tools project. GCP projects will automatically be deleted after 1 day.
# Other option: dev - this will allow for projects to persist for longer than 1 day
if [[ "${RBS_ENV}" == "tools" ]]; then
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
    RBS_POOLID=datarepo_v1
    RBS_INSTANCEURL=https://buffer.tools.integ.envs.broadinstitute.org
elif [[ "${RBS_ENV}" == "dev" ]]; then
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


echo "If you ran this script with 'source', the environment variables have been set in this context.
If you want these values to show up in intellij test/run profiles, you will need to set
these variables in your bash profile or set them manually in intellij profiles."


if [[ "${PRINT_ENV_VARS}" == "y" ]]; then
  echo "

  export AZURE_SYNAPSE_WORKSPACENAME=$AZURE_SYNAPSE_WORKSPACENAME
  export AZURE_CREDENTIALS_HOMETENANTID=$AZURE_CREDENTIALS_HOMETENANTID
  export AZURE_CREDENTIALS_APPLICATIONID=$AZURE_CREDENTIALS_APPLICATIONID
  export AZURE_CREDENTIALS_SECRET=$AZURE_CREDENTIALS_SECRET
  export AZURE_SYNAPSE_SQLADMINUSER=$AZURE_SYNAPSE_SQLADMINUSER
  export AZURE_SYNAPSE_SQLADMINPASSWORD=$AZURE_SYNAPSE_SQLADMINPASSWORD
  export AZURE_SYNAPSE_ENCRYPTIONKEY=$AZURE_SYNAPSE_ENCRYPTIONKEY
  export GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS
  export GOOGLE_SA_CERT=$GOOGLE_SA_CERT
  export RBS_ENABLED=$RBS_ENABLED
  export RBS_POOLID=$RBS_POOLID
  export RBS_INSTANCEURL=$RBS_INSTANCEURL
  export RBS_CLIENTCREDENTIALFILEPATH=$RBS_CLIENTCREDENTIALFILEPATH"
fi

