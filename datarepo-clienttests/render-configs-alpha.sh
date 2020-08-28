#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}
GOOGLE_APPLICATION_CREDENTIALS="/tmp/jade-alpha-account.json"
CLUSTER_NAME=jade-master-us-central1
GOOGLE_CLOUD_ZONE=us-central1
GOOGLE_CLOUD_PROJECT=terra-datarepo-alpha

#docker run --rm -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest \
#vault read -format=json secret/dsde/datarepo/alpha/datarepo-api-sa  | jq -r .data.key | base64 --decode \
#| tee ${GOOGLE_APPLICATION_CREDENTIALS} | \
#jq -r .private_key > /tmp/jade-alpha-account.pem



# authenticate against google cloud
#gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud container clusters get-credentials ${CLUSTER_NAME} \
--region ${GOOGLE_CLOUD_ZONE} --project ${GOOGLE_CLOUD_PROJECT}
# set default region and project
gcloud config set compute/zone ${GOOGLE_CLOUD_ZONE}
gcloud config set project ${GOOGLE_CLOUD_PROJECT}
