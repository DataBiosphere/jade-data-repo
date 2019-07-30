#!/bin/bash

: ${DB:?}
: ${ENVIRONMENT:?}
: ${SUFFIX:-$ENVIRONMENT}

VAULT_PATH="secret/dsde/datarepo/${ENVIRONMENT}/api-secrets-${SUFFIX}.json"
echo $VAULT_PATH
PW=$( vault read -format=json $VAULT_PATH | \
      jq -r .data.datarepoPassword )

kubectl --namespace data-repo run psql -it --serviceaccount=jade-sa --restart=Never --rm --image postgres:9.6 -- \
    psql "postgresql://drmanager:${PW}@cloudsql-proxy-service.data-repo/${DB}"
