#!/bin/bash

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

kubectl --namespace ${SUFFIX} run psql -it --serviceaccount=${SUFFIX}-jade-datarepo-api --restart=Never --rm --image postgres:9.6 -- \
    psql "postgresql://drmanager:${PW}@${SUFFIX}-jade-gcloud-sqlproxy.${SUFFIX}/${DB}"
