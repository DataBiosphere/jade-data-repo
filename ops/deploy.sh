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
NOW=$(date +%Y-%m-%d_%H-%M-%S)
DATA_REPO_TAG="${GOOGLE_CLOUD_PROJECT}_${NOW}"
SCRATCH=/tmp/deploy-scratch

# Install jq
command -v jq >/dev/null 2>&1 || {
    echo "jq not found, installing";
    brew install jq;
}

# Install kubectl
command -v kubectl >/dev/null 2>&1 || {
    echo "kubectl not found, installing";
    brew install kubernetes-cli;
}

# Make sure kubectl is at least the right version to support `kubectl wait ...`
KUBECTL_VERSION=$(kubectl version --output=json --client=True | jq -r .clientVersion.gitVersion)
function version_gt() { test "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1"; }
if version_gt "v1.14.0" $KUBECTL_VERSION; then
    echo "kubectl needs to be at least version 1.14.0, trying to update"
    brew upgrade kubernetes-cli
fi

# Install consul-template (note this didn't work with 0.20.0, I kept getting 403 errors)
command -v consul-template >/dev/null 2>&1 || {
    echo "consul-template not found, installing";
    curl https://releases.hashicorp.com/consul-template/0.19.5/consul-template_0.19.5_darwin_386.tgz | tar xzv;
    mv consul-template /usr/local/bin/;
}

# Make sure kubectl is pointing at the right project
KUBECTL_CONTEXT=$(kubectl config current-context)
if [[ $KUBECTL_CONTEXT != *${GOOGLE_CLOUD_PROJECT}* ]]; then
    echo "the kubernetes context (${KUBECTL_CONTEXT}) does not match your GOOGLE_CLOUD_PROJECT: ${GOOGLE_CLOUD_PROJECT}"
    echo "the easiest way to change it is using the context menu after clicking on the Docker icon in your top bar"
    exit 1
fi

# make a temporary directory for rendering, we'll delete it later
mkdir -p $SCRATCH

kubectl get namespace data-repo 2>/dev/null && kubectl delete namespace data-repo

# create a data-repo namespace to put everything in
kubectl apply -f "${WD}/k8s/namespace.yaml"

# create the pod security policies and service account
kubectl apply --namespace data-repo -f "${WD}/k8s/psp/service-account.yaml"

# TODO: I have a hunch that this only works on terraformed CIS k8s clusters, leaving commented out for now
kubectl apply --namespace data-repo -f "${WD}/k8s/psp"

# render secrets, create or update on kubernetes
consul-template -template "${WD}/k8s/secrets/api-secrets.yaml.ctmpl:${SCRATCH}/api-secrets.yaml" -once
kubectl apply -f "${SCRATCH}/api-secrets.yaml"

# TODO: use kubectl waits instead of sleeps
echo 'waiting 5 sec for secrets to be ready'
sleep 5

# update the service account key
vault read "secret/dsde/datarepo/${ENVIRONMENT}/sa-key.json" -format=json | jq .data > "${SCRATCH}/sa-key.json"
kubectl --namespace data-repo create secret generic sa-key --from-file="sa-key.json=${SCRATCH}/sa-key.json"

# set the tsl certificate
vault read -field=value secret/dsde/datarepo/${ENVIRONMENT}/common/server.crt > "${SCRATCH}/tls.crt"
vault read -field=value secret/dsde/datarepo/${ENVIRONMENT}/common/server.key > "${SCRATCH}/tls.key"
kubectl --namespace=data-repo create secret generic wildcard.datarepo.broadinstitute.org --from-file=${SCRATCH}/tls.key --from-file=${SCRATCH}/tls.crt


# create or update postgres pod + service
kubectl apply -f "${WD}/k8s/services"
kubectl apply -f "${WD}/k8s/pods/psql-pod.yaml"

# wait for the db to be ready so that we can run commands against it
echo 'waiting 10 sec for database to be up'
sleep 10
# TODO: add readiness probe to postgres pod def to check for port 5432 to be available
kubectl wait --for=condition=Ready -f "${WD}/k8s/pods/psql-pod.yaml"

# create the right databases/user/extensions (TODO: moving this to be the APIs responsibility soon)
cat "${WD}/../db/create-data-repo-db" | \
    kubectl --namespace data-repo run psql -i --serviceaccount=jade-sa --restart=Never --rm --image=postgres:9.6 -- \
    psql -h postgres-service.data-repo -U postgres

# create deployments
kubectl apply -f "${WD}/k8s/deployments/"

# build a docker container and push it to gcr
pushd ${WD}/..
GCR_TAG=$DATA_REPO_TAG .x/gradlew dockerPush
popd

kubectl --namespace data-repo set image deployments/api-deployment \
    "data-repo-api-container=gcr.io/broad-jade-dev/jade-data-repo:${DATA_REPO_TAG}"

# try to deploy the ui, assuming that the jade-data-repo-ui directory is a sibling of this jade-data-repo directory
UI_DIR="${WD}/../../jade-data-repo-ui"
if test -d "$UI_DIR"; then
    pushd $UI_DIR
    ./ops/deploy.sh
    popd
fi

rm -r $SCRATCH
