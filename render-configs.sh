#!/bin/bash
### How to call this script
# To write secrets to tmp files:
# ./render-configs.sh (defaults to dev azure, tools RBS)

# There are three optional arguments:
# ./render-configs.sh (Azure Synapse: -a dev|integration) (RBS: -r tools|dev)  (Put string of env variables in your clipboard to copy to intellij: -i)
# e.g.: ./render-configs.sh -a dev -r tools -i
# This would set azure synapse to dev, RBS to tools, print out the environment variables and put the variables in your clipboard

# If you're running Azure Integration Tests you should use the following settings:
# ./render-configs.sh -a integration -r tools
# Then, refresh your z-shell configuration (`source ~./zshrc`) (follow getting started doc to set env variables)
# Alternatively, if you use the -i flag, it copies the environment variables to your clipboard and you can paste them into your Intellij test profile.
# ./render-configs.sh -a integration -r tools -i

# If you want a set up locally, you can use the following settings:
# ./render-configs.sh -a dev -r dev
# Then, refresh your z-shell configuration (`source ~./zshrc`)
# ./gradlew bootRun

AZURE_ENV=dev
RBS_ENV=tools
COPY_INTELLIJ_ENV_VARS=n

while getopts ":a:r:i" option; do
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
    *)
      echo "Usage: $0 [-a (dev|integration)] [-r (tools|dev)] [-i]"
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
# writing this values to a tmp file so the value can match the set RBS environment
echo $AZURE_SYNAPSE_WORKSPACENAME > "/tmp/azure-synapse-workspacename.txt"


vault read -field=tenant-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-tenant-id.key"
AZURE_CREDENTIALS_HOMETENANTID=$(cat "/tmp/jade-dev-tenant-id.key")


vault read -field=client-id secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-client-id.key"
AZURE_CREDENTIALS_APPLICATIONID=$(cat "/tmp/jade-dev-client-id.key")


vault read -field=client-secret secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-azure.key"
AZURE_CREDENTIALS_SECRET=$(cat "/tmp/jade-dev-azure.key")


vault read -field=synapse-sql-admin-user secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-synapse-admin-user.key"
AZURE_SYNAPSE_SQLADMINUSER=$(cat "/tmp/jade-dev-synapse-admin-user.key")


vault read -field=synapse-sql-admin-password secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-synapse-admin-password.key"
AZURE_SYNAPSE_SQLADMINPASSWORD=$(cat "/tmp/jade-dev-synapse-admin-password.key")


vault read -field=synapse-encryption-key secret/dsde/datarepo/"$AZURE_ENV"/azure-application-secrets \
    > "/tmp/jade-dev-synapse-encryption-key.key"
AZURE_SYNAPSE_ENCRYPTIONKEY=$(cat "/tmp/jade-dev-synapse-encryption-key.key")

# ========================
# Google Credentials
# ========================
vault read -format=json secret/dsde/datarepo/dev/sa-key.json \
    | jq .data | tee /tmp/jade-dev-account.json \
    | jq -r .private_key > /tmp/jade-dev-account.pem

GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
GOOGLE_SA_CERT=/tmp/jade-dev-account.pem

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
# writing these values to tmp files so the value can match the set RBS environment
echo $RBS_POOLID > "/tmp/rbs-pool-id.txt"
echo $RBS_INSTANCEURL > "/tmp/rbs-instance-url.txt"

RBS_CLIENTCREDENTIALFILEPATH=/tmp/buffer-client-sa-account.json

vault read -field=key "$BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH" \
    | base64 -d > "$RBS_CLIENTCREDENTIALFILEPATH"

# Azure B2C authentication settings
# Setting these variables so that can be put into clipboard for intellij profile setup
OIDC_ADDCLIENTIDTOSCOPE=true
OIDC_AUTHORITYENDPOINT=https://oauth-proxy.dsp-eng-tools.broadinstitute.org/b2c
OIDC_CLIENTID=bbd07d43-01cb-4b69-8fd0-5746d9a5c9fe
OIDC_EXTRAAUTHPARAMS='prompt=login'
OIDC_PROFILEPARAM=b2c_1a_signup_signin_tdr_dev


if [[ "${COPY_INTELLIJ_ENV_VARS}" == "y" ]]; then
  echo "AZURE_SYNAPSE_WORKSPACENAME=$AZURE_SYNAPSE_WORKSPACENAME;AZURE_CREDENTIALS_HOMETENANTID=$AZURE_CREDENTIALS_HOMETENANTID;AZURE_CREDENTIALS_APPLICATIONID=$AZURE_CREDENTIALS_APPLICATIONID;AZURE_CREDENTIALS_SECRET=$AZURE_CREDENTIALS_SECRET;AZURE_SYNAPSE_SQLADMINUSER=$AZURE_SYNAPSE_SQLADMINUSER;AZURE_SYNAPSE_SQLADMINPASSWORD='$AZURE_SYNAPSE_SQLADMINPASSWORD';AZURE_SYNAPSE_ENCRYPTIONKEY=$AZURE_SYNAPSE_ENCRYPTIONKEY;GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS;GOOGLE_SA_CERT=$GOOGLE_SA_CERT;RBS_POOLID=$RBS_POOLID;RBS_INSTANCEURL=$RBS_INSTANCEURL;OIDC_AUTHORITYENDPOINT=$OIDC_AUTHORITYENDPOINT;OIDC_CLIENTID=$OIDC_CLIENTID;OIDC_EXTRAAUTHPARAMS=$OIDC_EXTRAAUTHPARAMS;OIDC_PROFILEPARAM=$OIDC_PROFILEPARAM"  | pbcopy
  echo "Environment variables copied to clipboard"
fi

unset AZURE_ENV
unset RBS_ENV
unset COPY_INTELLIJ_ENV_VARS

