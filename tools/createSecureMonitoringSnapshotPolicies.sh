#!/bin/bash

# This script creates a "protected-data" policy for snapshots that have `secureMonitoringEnabled`.
# The script goes through the following steps:
# 1. Authenticate as your Data Repo admin user
# 2. Get snapshots from the Data Repo snapshots GET endpoint
# 3. Filter the response for snapshots with `secureMonitoringEnabled` and get their ids
# 4. Authenticate as the Data Repo service account (required for TPS)
# 5. For each snapshot id, create a "protected-data" policy in the Terra Policy Service.

echo "This script will create a protected-data policy for snapshots with secureMonitoringEnabled."
PS3="❓ Select an environment:"
select ENV in dev alpha staging prod; do
  case $ENV in
  dev)
    DATAREPO_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/datarepo/dev/sa-key.json"
    DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH="/tmp/jade-dev-account.json"
    DATAREPO_URL="https://jade.datarepo-dev.broadinstitute.org"
    TPS_URL="https://tps.dsde-dev.broadinstitute.org"
    ;;
  alpha)
    DATAREPO_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/datarepo/alpha/datarepo-api-sa"
    DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH="/tmp/jade-alpha-account.json"
    DATAREPO_URL="https://data.alpha.envs-terra.bio"
    TPS_URL=""
    ;;
  staging)
    DATAREPO_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/datarepo/staging/datarepo-api-sa"
    DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH="/tmp/jade-staging-account.json"
    DATAREPO_URL="https://data.staging.envs-terra.bio"
    TPS_URL=""
    ;;
  prod)
    DATAREPO_SERVICE_ACCOUNT_VAULT_PATH="secret/dsde/datarepo/production/datarepo-api-sa"
    DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH="/tmp/jade-prod-account.json"
    DATAREPO_URL="https://data.terra.bio"
    TPS_URL=""
    ;;
  esac
  break
done
printf "\n"

gcloud auth login
TOKEN=$(gcloud auth print-access-token)

echo "Retrieving snapshots with secureMonitoringEnabled..."
SNAPSHOTS=$(curl -s -X GET "${DATAREPO_URL}/api/repository/v1/snapshots?direction=desc&limit=4000&offset=0&sort=created_date" -H "accept: application/json" -H "authorization: Bearer ${TOKEN}" | jq -r '.items[] | select(.secureMonitoringEnabled == true) | .id' | tr "\n" " ")
IFS=" " read -r -a SNAPSHOTS_ARR <<< "$SNAPSHOTS"
N_SNAPSHOTS=${#SNAPSHOTS_ARR[@]}
echo "${N_SNAPSHOTS} snapshots found in ${ENV} with secureMonitoringEnabled"
if [ "$N_SNAPSHOTS" -eq 0 ]; then
  exit 0
fi

echo "Activating Data Repo service account (required to authenticate to TPS)..."
if [ "${ENV}" == "dev" ]; then
  vault read -format=json "${DATAREPO_SERVICE_ACCOUNT_VAULT_PATH}" | jq .data | tee "${DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH}"
else
  vault read -format=json "${DATAREPO_SERVICE_ACCOUNT_VAULT_PATH}" | jq -r .data.key | base64 --decode | tee "${DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH}"
fi
gcloud auth activate-service-account --key-file "${DATAREPO_SERVICE_ACCOUNT_OUTPUT_PATH}"
SA_TOKEN=$(gcloud auth print-access-token)

if [ "${ENV}" != "dev" ]; then
  echo "Skipping policy creation (TPS only exists in dev)"
  exit 0
fi

for SNAPSHOT_ID in "${SNAPSHOTS_ARR[@]}"
do
    echo "Creating a protected data policy for snapshot: ${SNAPSHOT_ID}..."
    curl -X POST "${TPS_URL}/api/policy/v1alpha1/pao" \
      -H "accept: */*" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${SA_TOKEN}" \
      -d '{"objectId": '"${SNAPSHOT_ID}"',
      "component": "TDR",
      "objectType": "snapshot",
      "attributes": {
        "inputs": [{
            "namespace": "terra",
            "name": "protected-data"
          }]
      }
    }'
done
