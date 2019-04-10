#!/bin/bash

set -e

# Prerequisites:
# - Docker (https://www.docker.com/products/docker-desktop)
# - Homebrew (https://brew.sh/)

# check to make sure vault and cloud env vars are set correctly
# these commands below check to make sure that an environment variable is set to a non-empty thing
: ${GOOGLE_CLOUD_PROJECT:?}
: ${VAULT_ADDR:?}
: ${ENVIRONMENT:?}

if [ -z "$VAULT_TOKEN" ]; then
    if [ ! -f ~/.vault-token ]; then
        echo "VAULT_TOKEN needs to be set or ~/.vault-token needs to exist"
        exit 1
    fi
    export VAULT_TOKEN=$( cat ~/.vault-token )
fi

# the paths we'll use will be relative to this script
WD=$( dirname "${BASH_SOURCE[0]}" )
SCRATCH=/tmp/deploy-scratch

# Install kubectl
command -v kubectl >/dev/null 2>&1 || {
    echo "kubectl not found, installing";
    brew install kubernetes-cli;
}

# Install consul-template (note this didn't work with 0.20.0, I kept getting 403 errors)
command -v consul-template >/dev/null 2>&1 || {
    echo "consul-template not found, installing";
    curl https://releases.hashicorp.com/consul-template/0.19.5/consul-template_0.19.5_darwin_386.tgz | tar xzv;
    mv consul-template /usr/local/bin/;
}

# make a temporary directory for rendering, we'll delete it later
mkdir -p $SCRATCH

# create a data-repo namespace to put everything in
kubectl apply -f "${WD}/../kubernetes/namespace.yaml"

# render secrets, create or update on kubernetes
consul-template -template "${WD}/secrets/api-secrets.yaml.ctmpl:${SCRATCH}/api-secrets.yaml" -once
kubectl apply -f "${SCRATCH}/api-secrets.yaml"

# update the service account key
vault read "secret/dsde/firecloud/${ENVIRONMENT}/datarepo/sa-key${GOOGLE_CLOUD_PROJECT}.json" -format=json | jq .data  > "${SCRATCH}/sa-key.json"
kubectl --namespace data-repo get secret sa-key && kubectl --namespace data-repo delete secret sa-key
kubectl --namespace data-repo create secret generic sa-key --from-file="sa-key.json=${SCRATCH}/sa-key.json"

# set the tsl certificate
vault read -field=value secret/dsp/certs/wildcard.datarepo-dev.broadinstitute.org/20210326/server.crt > "${SCRATCH}/server.crt"
vault read -field=value secret/dsp/certs/wildcard.datarepo-dev.broadinstitute.org/20210326/server.key > "${SCRATCH}/server.key"
kubectl get secret tls-cert && kubectl delete secret tls-cert
kubectl create secret tls tls-cert --cert=${SCRATCH}/server.crt --key=${SCRATCH}/server.key

# create or update postgres pod + service
kubectl apply -f "${WD}/pods/psql-pod.yaml"
kubectl apply -f "${WD}/services/psql-service.yaml"

# create the right databases/user/extensions (TODO: moving this to be the APIs responsibility soon)
cat "${WD}/../../db/create-data-repo-db" | \
    kubectl --namespace data-repo run psql -i --restart=Never --rm --image=postgres:9.6 -- psql -h postgres-service -U postgres

# prepare the code to be dockerized
${WD}/../../gradlew dockerPush

# create or update the api pod + service
#kubectl get pod data-repo-api && kubectl delete pod data-repo-api
kubectl apply -f "${WD}/pods/api-pod.yaml"
kubectl apply -f "${WD}/services/api-service.yaml"

kubectl --namespace data-repo set image pods/data-repo-api "data-repo-api-container=gcr.io/broad-jade-dev/jade-data-repo:latest${GOOGLE_CLOUD_PROJECT}"

# create or update oidc proxy pod + service, probably need to render secrets
kubectl apply -f "${WD}/pods/oidc-proxy-no-ldap-pod.yaml"
kubectl apply -f "${WD}/services/oidc-proxy-service.yaml"

rm -r $SCRATCH
