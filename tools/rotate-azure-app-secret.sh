#!/bin/bash
# This script takes in a new Azure application secret and applies it to all environments
# To generate a secret:
# - Log into the Azure portal (https://portal.azure.com/)
# - Go to the DSP Terra Dev tenant
# - Go to the App Registration service
# - Click on the TDR application (https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/Credentials/appId/22cb243c-f1a5-43d8-8f12-6566bcce6542/isMSAApp/)
# - Go to the “Certificates & Secrets” blade
# - Create a new secret and copy it (you can’t ever see it again)
#
# Once the secret is generated, run this script:
#
# ./rotate-azure-app-secret.sh <your new token>
#
# and verify that the secrets have been properly updated.
# Long running services (e.g. dev, alpha, staging, production) all need to be synced in Argo

AZURE_APP_SECRET=$1

update_secret () {
  local location_name="$1"
  local secret_location="$2"

  echo "Updating secret for environment $location_name"
  vault read --format=json --field=data $secret_location | jq --arg AZURE_APP_SECRET "$AZURE_APP_SECRET" '.applicationsecret2=$AZURE_APP_SECRET' | vault write ${secret_location} -
}


update_secret "dev" "secret/dsde/datarepo/dev/helm-azure"
update_secret "perf" "secret/dsde/datarepo/perf/helm-azure"
update_secret "integration" "secret/dsde/datarepo/integration/helm-azure-integration"
update_secret "alpha" "secret/dsde/datarepo/alpha/helm-azure"
update_secret "staging" "secret/dsde/datarepo/staging/helm-azure"
update_secret "production" "secret/dsde/datarepo/production/helm-azure"
