#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}
export GOOGLE_APPLICATION_CREDENTIALS="/tmp/jade-dev-account.json"
CLUSTER_NAME=dev-master
GOOGLE_CLOUD_ZONE=us-central1
GOOGLE_CLOUD_PROJECT=broad-jade-dev

docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
    vault read -format=json secret/dsde/datarepo/dev/sa-key.json | \
    jq .data | tee /tmp/jade-dev-account.json | \
    jq -r .private_key > /tmp/jade-dev-account.pem



# authenticate against google cloud
gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud container clusters get-credentials ${CLUSTER_NAME} \
--region ${GOOGLE_CLOUD_ZONE} --project ${GOOGLE_CLOUD_PROJECT}
# set default region and project
gcloud config set compute/zone ${GOOGLE_CLOUD_ZONE}
gcloud config set project ${GOOGLE_CLOUD_PROJECT}
