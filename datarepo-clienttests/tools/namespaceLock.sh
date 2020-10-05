#!/bin/bash

namespace=$1

if [ -z "$namespace" ]
then
  echo "LockNamespace: namespace cannot be empty"
  exit 1
fi

if kubectl get secrets -n ${namespace} ${namespace}-inuse > /dev/null 2>&1; then
    printf "LockNamespace FAILED: Namepsace ${namespace} already in use.\n"
    exit 1
else
    printf "LockNamespace Namespace ${namespace} not in use\n"
    kubectl create secret generic ${namespace}-inuse --from-literal=inuse=${namespace} -n ${namespace}
fi
