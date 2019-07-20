#!/bin/bash

set -e

: ${GOOGLE_CLOUD_PROJECT:?}
: ${VAULT_ADDR:?}
: ${ENVIRONMENT:?}
: ${SUFFIX:?}

# the paths we'll use will be relative to this script
WD=$( dirname "${BASH_SOURCE[0]}" )
NOW=$(date +%Y-%m-%d_%H-%M-%S)
DATA_REPO_TAG="${GOOGLE_CLOUD_PROJECT}_${NOW}"
SCRATCH=/tmp/deploy-scratch

# Make sure kubectl is pointing at the right project
KUBECTL_CONTEXT=$(kubectl config current-context)
if [[ $KUBECTL_CONTEXT != *${GOOGLE_CLOUD_PROJECT}* ]]; then
    echo "the kubernetes context (${KUBECTL_CONTEXT}) does not match your GOOGLE_CLOUD_PROJECT: ${GOOGLE_CLOUD_PROJECT}"
    echo "the easiest way to change it is using the context menu after clicking on the Docker icon in your top bar"
    exit 1
fi

# make a temporary directory for rendering, we'll delete it later
mkdir -p $SCRATCH

# render environment-specific oidc deployment and ingress configs then create them
consul-template -template "${WD}/k8s/deployments/oidc-proxy-deployment.yaml.ctmpl:${SCRATCH}/oidc-proxy-deployment.yaml" -once
kubectl --namespace=data-repo apply -f "${SCRATCH}/oidc-proxy-deployment.yaml"

rm -r $SCRATCH
