#!/bin/bash

# Call this to connect to cloud Postgres for a specific Data Repo instance.
# For example, to connect to the dev datarepo database as user mm, run:
# $ DB=datarepo SUFFIX=mm ENVIRONMENT=dev ./db-connect.sh
# To connect to a user instance's stairway database, run:
# $ DB=stairway-mm SUFFIX=mm ENVIRONMENT=dev ./db-connect.sh
#
# If the connection times out, check that you are on the Broad VPN and are connected to correct
# Kubernetes cluster.
#
# If you get an AlreadyExists error, it's possible the pod didn't shut down properly. You can
# delete it using kubectl, where ZZ is the SUFFIX.
# $ kubectl --namespace ZZ delete pod ZZ-psql

: "${DB:?}"
: "${ENVIRONMENT:?}"
SUFFIX=${SUFFIX:-$ENVIRONMENT}

VAULT_PATH="secret/dsde/datarepo/${ENVIRONMENT}/helm-datarepodb-${ENVIRONMENT}"

PW=$( vault read -format=json "$VAULT_PATH" | jq -r .data.datarepopassword )

if [ -z "$PW" ]; then
  echo "Vault password is empty"
  exit 1 # error
fi

kubectl --namespace "${SUFFIX}" run "${SUFFIX}-psql" -it --restart=Never --rm --image postgres:11 -- \
    psql "postgresql://drmanager:${PW}@${SUFFIX}-jade-gcloud-sqlproxy.${SUFFIX}/${DB}"
