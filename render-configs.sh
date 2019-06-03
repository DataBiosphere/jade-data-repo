#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

render () {
    file=$1
    destfile=$2
    docker run --rm -v ${PWD}/src/main/resources:/working -e VAULT_TOKEN=$VAULT_TOKEN broadinstitute/dsde-toolbox:latest consul-template -config=/etc/consul-template/config/config.json -template=/working/${file}:/working/${destfile} -once
}

render application-secrets.properties.ctmpl application-secrets.properties
render jade-dev-account.pem.ctmpl jade-dev-account.pem
cp ${PWD}/src/main/resources/jade-dev-account.pem /tmp/jade-dev-account.pem
