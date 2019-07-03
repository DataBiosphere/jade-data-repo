#!/bin/bash

set -e

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

# render secrets, create or update on kubernetes
consul-template -template "${WD}/k8s/secrets/api-secrets.yaml.ctmpl:${SCRATCH}/api-secrets.yaml" -once
kubectl apply -f "${SCRATCH}/api-secrets.yaml"

rm -r $SCRATCH
