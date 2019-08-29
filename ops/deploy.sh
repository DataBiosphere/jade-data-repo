#!/bin/bash

set -e

# Prerequisites:
# - Docker (https://www.docker.com/products/docker-desktop)
# - Homebrew (https://brew.sh/)
# - Node 10.5 (https://nodejs.org)

# check to make sure vault and cloud env vars are set correctly
# these commands below check to make sure that an environment variable is set to a non-empty thing
: ${GOOGLE_CLOUD_PROJECT:?}
: ${VAULT_ADDR:?}
: ${ENVIRONMENT:?}
: ${SUFFIX:?}

if [ -z "$VAULT_TOKEN" ]; then
    if [ ! -f ~/.vault-token ]; then
        echo "VAULT_TOKEN needs to be set or ~/.vault-token needs to exist"
        exit 1
    fi
    export VAULT_TOKEN=$( cat ~/.vault-token )
fi

# the paths we'll use will be relative to this script
export KUBE_NAMESPACE=${KUBE_NAMESPACE:-'data-repo'}
SIDECAR_IMAGE_TAG=${SIDECAR_IMAGE_TAG:-'0.4.3'}
WD=$( dirname "${BASH_SOURCE[0]}" )
NOW=$(date +%Y-%m-%d_%H-%M-%S)
DATA_REPO_TAG="${GOOGLE_CLOUD_PROJECT}_${NOW}"
SCRATCH=/tmp/deploy-scratch
FROM=yaml.ctmpl
TO=yaml

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
    echo "kubectl needs to be at least version 1.14.0, please upgrade it using brew"
    exit 1
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

#"yaml" is alway assume vs "yml"
#render configs to apply dir
find ${WD} -name "*.ctmpl" -type f -exec sh -c 'consul-template -once -log-level=err -template="$4":$3/"${4%.$1}.$2"' sh "$FROM" "$TO" "$SCRATCH" {} ';'
find ${WD} -name "*.yaml" -type f -exec sh -c 'consul-template -once -log-level=err -template="$2":$1/"$2"' sh "$SCRATCH" {} ';'

kubectl get namespace "${KUBE_NAMESPACE}" 2>/dev/null && kubectl delete namespace "${KUBE_NAMESPACE}"

# create a namespace to put everything in
kubectl create namespace "${KUBE_NAMESPACE}"

# render secrets, create or update on kubernetes
kubectl --namespace="${KUBE_NAMESPACE}" apply -f "${SCRATCH}/ops/k8s/configs"

# create service account and pod security policy
kubectl --namespace="${KUBE_NAMESPACE}" apply -f "${SCRATCH}/ops/k8s/psp"

# TODO: use kubectl waits instead of sleeps
echo 'waiting 5 sec for secrets to be ready'
sleep 5

# update the service account key
vault read "secret/dsde/datarepo/${ENVIRONMENT}/sa-key.json" -format=json | jq .data > "${SCRATCH}/sa-key.json"
kubectl --namespace="${KUBE_NAMESPACE}" create secret generic sa-key --from-file="sa-key.json=${SCRATCH}/sa-key.json"

# update the sql proxy service account key
vault read "secret/dsde/datarepo/${ENVIRONMENT}/proxy-sa-${SUFFIX}.json" -format=json | jq .data > "${SCRATCH}/proxy-sa.json"
kubectl --namespace="${KUBE_NAMESPACE}" create secret generic sql-proxy-sa --from-file="proxy-sa.json=${SCRATCH}/proxy-sa.json"

# set the tls certificate
server_crt=$(docker run --rm -it -v "$PWD":/working -v ${HOME}/.vault-token:/root/.vault-token broadinstitute/dsde-toolbox vault read --format=json secret/dsde/datarepo/${ENVIRONMENT}/common/server.crt | jq -r .data.value | tr -d '\r')
# TODO: For each env, move intermediate crt from secret/dsp/certs/wildcard.dsde-<env>.broadinstitute.org/<expiration>/server.intermediate.crt secret/dsde/firecloud/${ENVIRONMENT}/common/server.intermediate.crt so that you  don't have to guess the date (20200608) to get it for any environment
server_intermediate=$(docker run --rm -it -v "$PWD":/working -v ${HOME}/.vault-token:/root/.vault-token broadinstitute/dsde-toolbox vault read --format=json secret/dsde/datarepo/${ENVIRONMENT}/common/server.intermediate.crt | jq -r .data.value | tr -d '\r')
ca_bundle="$(cat <<EOF
${server_crt}
${server_intermediate}
EOF
)"
echo "${ca_bundle}" > ${SCRATCH}/tls.crt
docker run --rm -it -v "$PWD":/working -v ${HOME}/.vault-token:/root/.vault-token broadinstitute/dsde-toolbox vault read --format=json secret/dsde/datarepo/${ENVIRONMENT}/common/server.key | jq -r .data.value | tr -d '\r' > ${SCRATCH}/tls.key
kubectl --namespace="${KUBE_NAMESPACE}" create secret generic wildcard.datarepo.broadinstitute.org --from-file=${SCRATCH}/tls.key --from-file=${SCRATCH}/tls.crt

rm ${SCRATCH}/tls.crt ${SCRATCH}/tls.key

# create services
kubectl --namespace="${KUBE_NAMESPACE}" apply -f "${SCRATCH}/ops/k8s/services"

# create Deployments
kubectl --namespace="${KUBE_NAMESPACE}" apply -f "${SCRATCH}/ops/k8s/deployments"

# build a docker container and push it to gcr
pushd ${WD}/..
GCR_TAG=$DATA_REPO_TAG ./gradlew jib
popd

kubectl --namespace="${KUBE_NAMESPACE}" set image deployments/api-deployment \
    "data-repo-api-container=gcr.io/broad-jade-dev/jade-data-repo:${DATA_REPO_TAG}"

# try to deploy the ui, assuming that the jade-data-repo-ui directory is a sibling of this jade-data-repo directory
UI_DIR="${WD}/../../jade-data-repo-ui"
if test -d "$UI_DIR"; then
    pushd $UI_DIR
    ./ops/deploy.sh
    popd
fi

rm -r $SCRATCH
