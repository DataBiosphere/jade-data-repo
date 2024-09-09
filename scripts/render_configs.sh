#!/bin/bash
### How to call this script
# To write secrets to tmp files:
# ./render_configs.sh (defaults to dev azure, tools RBS)

# There are three optional arguments:
# ./render_configs.sh (Azure Synapse: -a dev|integration) (RBS: -r tools|dev)  (Put string of env variables in your clipboard to copy to intellij: -i)
# e.g.: ./render_configs.sh -a dev -r tools -i
# This would set azure synapse to dev, RBS to tools, and put the variables in your clipboard

# If you're running Azure Integration Tests you should use the following settings:
# ./render_configs.sh -a integration -r tools
# Then, refresh your z-shell configuration (`source ~/.zshrc`) (follow getting started doc to set env variables)
# Alternatively, if you use the -i flag, it copies the environment variables to your clipboard and you can paste them into your Intellij test profile.
# ./render_configs.sh -a integration -r tools -i

# If you want a set up locally, you can use the following settings:
# ./render_configs.sh -a dev -r dev
# Then, refresh your z-shell configuration (`source ~/.zshrc`)
# ./gradlew bootRun

set -eu

AZURE_ENV=dev
RBS_ENV=tools
COPY_INTELLIJ_ENV_VARS=n
ECHO_SETTINGS_TO_OUTPUT=n
USE_VAULT="${USE_VAULT:-false}"

while getopts ":a:r:ie" option; do
  case $option in
    a)
      AZURE_ENV=$OPTARG
      ;;
    r)
      RBS_ENV=$OPTARG
      ;;
    i)
      COPY_INTELLIJ_ENV_VARS=y
      ;;
    e)
      ECHO_SETTINGS_TO_OUTPUT=y
      ;;
    *)
      echo "Usage: $0 [-a (dev|integration)] [-r (tools|dev)] [-i] [-e]"
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
    GCLOUD_PROJECT=broad-jade-dev
elif [[ "${AZURE_ENV}" == "integration" ]]; then
    AZURE_SYNAPSE_WORKSPACENAME=tdr-snps-int-east-us-ondemand.sql.azuresynapse.net
    GCLOUD_PROJECT=broad-dsde-qa
else
    echo "Invalid Azure environment: $AZURE_ENV"
    exit 1
fi
# writing this values to a tmp file so the value can match the set azure environment
echo $AZURE_SYNAPSE_WORKSPACENAME > "/tmp/azure-synapse-workspacename.txt"

if $USE_VAULT; then
  AZURE_SECRETS=$(vault read -format=json -field=data secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets)
else
  AZURE_SECRETS=$(gcloud secrets versions access latest --project $GCLOUD_PROJECT --secret azure-secrets)
fi

AZURE_CREDENTIALS_HOMETENANTID=$(echo "$AZURE_SECRETS" | jq -r '."tenant-id"' | tee /tmp/jade-dev-tenant-id.key)
AZURE_CREDENTIALS_APPLICATIONID=$(echo "$AZURE_SECRETS" | jq -r '."client-id"' | tee /tmp/jade-dev-client-id.key)
AZURE_CREDENTIALS_SECRET=$(echo "$AZURE_SECRETS" | jq -r '."client-secret"' | tee /tmp/jade-dev-azure.key)
AZURE_SYNAPSE_SQLADMINUSER=$(echo "$AZURE_SECRETS" | jq -r '."synapse-sql-admin-user"' | tee /tmp/jade-dev-synapse-admin-user.key)
AZURE_SYNAPSE_SQLADMINPASSWORD=$(echo "$AZURE_SECRETS" | jq -r '."synapse-sql-admin-password"' | tee /tmp/jade-dev-synapse-admin-password.key)
AZURE_SYNAPSE_ENCRYPTIONKEY=$(echo "$AZURE_SECRETS" | jq -r '."synapse-encryption-key"' | tee /tmp/jade-dev-synapse-encryption-key.key)

# ========================
# Google Credentials
# ========================
if $USE_VAULT; then
  vault read -field=data -format=json secret/dsde/datarepo/dev/sa-key.json \
    | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem
else
  gcloud secrets versions access latest --project $GCLOUD_PROJECT --secret sa-b64 \
    | jq -r .sa \
    | base64 -d \
    | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem
fi

GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
GOOGLE_SA_CERT=/tmp/jade-dev-account.pem

# ========================
# Resource Buffer Service
# ========================
# By default, RBS will use the tools project. GCP projects will automatically be deleted after 1 day.
# Other option: dev - this will allow for projects to persist for longer than 1 day
if [[ "${RBS_ENV}" == "tools" ]]; then
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/integration/tools/buffer/client-sa
    BUFFER_CLIENT_SERVICE_ACCOUNT_GSM_PROJECT=broad-dsde-qa
    BUFFER_CLIENT_GSM_SECRET=buffer-client-sa-b64-integration
    RBS_POOLID=datarepo_v1
    RBS_INSTANCEURL=https://buffer.tools.integ.envs.broadinstitute.org
elif [[ "${RBS_ENV}" == "dev" ]]; then
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH=secret/dsde/terra/kernel/dev/dev/buffer/client-sa
    BUFFER_CLIENT_SERVICE_ACCOUNT_GSM_PROJECT=broad-jade-dev
    BUFFER_CLIENT_GSM_SECRET=buffer-client-sa-b64
    RBS_POOLID=datarepo_v3
    RBS_INSTANCEURL=https://buffer.dsde-dev.broadinstitute.org
else
    echo "Invalid RBS environment: $RBS_ENV - only 'tools' and 'dev' are supported."
    exit 1
fi
# writing these values to tmp files so the value can match the set RBS environment
echo $RBS_POOLID > "/tmp/rbs-pool-id.txt"
echo $RBS_INSTANCEURL > "/tmp/rbs-instance-url.txt"

RBS_CLIENTCREDENTIALFILEPATH=/tmp/buffer-client-sa-account.json

if $USE_VAULT; then
  vault read -field=key "$BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH" \
    | base64 -d > "$RBS_CLIENTCREDENTIALFILEPATH"
else
  gcloud secrets versions access latest --project $BUFFER_CLIENT_SERVICE_ACCOUNT_GSM_PROJECT --secret $BUFFER_CLIENT_GSM_SECRET \
    | jq -r '.key' | base64 -d > "$RBS_CLIENTCREDENTIALFILEPATH"
fi


VARIABLE_NAMES=(AZURE_SYNAPSE_WORKSPACENAME AZURE_CREDENTIALS_HOMETENANTID AZURE_CREDENTIALS_APPLICATIONID AZURE_CREDENTIALS_SECRET AZURE_SYNAPSE_SQLADMINUSER AZURE_SYNAPSE_SQLADMINPASSWORD AZURE_SYNAPSE_ENCRYPTIONKEY GOOGLE_APPLICATION_CREDENTIALS GOOGLE_SA_CERT RBS_POOLID RBS_INSTANCEURL)

if [[ "${COPY_INTELLIJ_ENV_VARS}" == "y" ]]; then
  SETTINGS=""
  for VAR_NAME in "${VARIABLE_NAMES[@]}"; do
    # Append the variable name and its value to the string
    SETTINGS+="$VAR_NAME=${!VAR_NAME};"
  done

  # Copy variables to clipboard
  echo "# Environment variables copied to clipboard"
  echo "$SETTINGS"  | pbcopy
fi

if [[ "${ECHO_SETTINGS_TO_OUTPUT}" == "y" ]]; then
  # Initialize an empty string
  OUTPUT=""

  # Loop over the array
  for VAR_NAME in "${VARIABLE_NAMES[@]}"; do
    # Append the variable name and its value to the string
    OUTPUT+="export $VAR_NAME='${!VAR_NAME}';"
  done

  echo "$OUTPUT"
fi

unset AZURE_ENV
unset RBS_ENV
unset COPY_INTELLIJ_ENV_VARS
unset ECHO_SETTINGS_TO_OUTPUT

