#!/bin/bash
set -eu

# Use this script to get the RBS Bearer token to use to auth w/ RBS swagger page
# Swagger page allows you to check how many projects are available for the pool
LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

echo "This script supports the following environments:"
PS3="❓ RBS environment: "
select ENV in tools dev alpha staging prod; do
  case $ENV in
  tools)
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/terra/kernel/integration/tools/buffer/client-sa"
    RBS_SWAGGER_URL="https://buffer.tools.integ.envs.broadinstitute.org/swagger-ui.html"
    RBS_POOL_ID="datarepo_v1"
    ;;
  dev)
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/terra/kernel/dev/dev/buffer/client-sa"
    RBS_SWAGGER_URL="https://buffer.dsde-dev.broadinstitute.org/swagger-ui.html"
    RBS_POOL_ID="datarepo_v3"
    ;;
  alpha)
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/terra/kernel/alpha/alpha/buffer/client-sa"
    RBS_SWAGGER_URL="https://buffer.dsde-alpha.broadinstitute.org/swagger-ui.html"
    RBS_POOL_ID="datarepo_v1"
    ;;
  staging)
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/terra/kernel/staging/staging/buffer/client-sa"
    RBS_SWAGGER_URL="https://buffer.dsde-staging.broadinstitute.org/swagger-ui.html"
    RBS_POOL_ID="datarepo_v1"
    ;;
  prod)
    BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH="secret/suitable/terra/kernel/prod/prod/buffer/client-sa"
    RBS_SWAGGER_URL="https://buffer.dsde-prod.broadinstitute.org/swagger-ui.html"
    RBS_POOL_ID="datarepo_v1"
    ;;
  esac
  echo "Will get secret from $BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH"
  break
done
printf "\n"

BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH=/tmp/buffer-client-sa-account.json

docker run --rm -e VAULT_TOKEN="${VAULT_TOKEN}" broadinstitute/dsde-toolbox:latest \
    vault read -field=key ${BUFFER_CLIENT_SERVICE_ACCOUNT_VAULT_PATH} | \
    base64 -d > ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}

gcloud auth activate-service-account --key-file ${BUFFER_CLIENT_SERVICE_ACCOUNT_OUTPUT_PATH}

gcloud auth print-access-token

printf "\n"
echo "Use the above bearer token here: ${RBS_SWAGGER_URL}"
echo "With pool Id ${RBS_POOL_ID} (Note: this may be out of date. Get the latest from the RBS repo: https://github.com/DataBiosphere/terra-resource-buffer/tree/master/src/main/resources/config)"
printf "\n"
echo "NOTE: Make sure to log back in with your desired user (i.e. gcloud auth login OR glcoud auth set account <email>)"