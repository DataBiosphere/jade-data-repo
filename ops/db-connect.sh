#!/bin/bash

## Call this to connect to PostGres for a specific Data Repo instance.
## e.g. DB=datarepo SUFFIX=mm ENVIRONMENT=dev ./db-connect.sh
## If the connection times out, check that you are on the Broad VPN and are connected to correct Kubernetes cluster.

: ${DB:?}
: ${ENVIRONMENT:?}
SUFFIX=${SUFFIX:-$ENVIRONMENT}

VAULT_PATH="secret/dsde/datarepo/${ENVIRONMENT}/helm-datarepodb-${ENVIRONMENT}"
echo $VAULT_PATH
PW=$( vault read -format=json $VAULT_PATH | \
      jq -r .data.datarepopassword )

if [ -z "$PW" ]
then
  echo "Vault password is empty"
  exit 1 # error
fi

kubectl --namespace ${SUFFIX} run ${SUFFIX}-psql -it --serviceaccount=${SUFFIX}-jade-datarepo-api --restart=Never --rm --image postgres:9.6 -- \
    psql "postgresql://drmanager:${PW}@${SUFFIX}-jade-gcloud-sqlproxy.${SUFFIX}/${DB}"
