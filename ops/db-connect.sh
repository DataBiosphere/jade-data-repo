#!/bin/bash

: ${DB:?}

PW=$( vault read -format=json secret/dsde/datarepo/integration/api-secrets-integration.json | \
      jq -r .data.datarepoPassword )

kubectl --namespace data-repo run psql -it --serviceaccount=jade-sa --restart=Never --rm --image postgres:9.6 -- \
    psql "postgresql://drmanager:${PW}@cloudsql-proxy-service.data-repo/${DB}"
