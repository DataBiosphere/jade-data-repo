#!/bin/bash

clusterShortName=$1
region=$2
project=$3
namespace=$4

if [ -z "$clusterShortName" ]
then
  echo "LockNamespace: clusterShortName cannot be empty"
  exit 1
fi
if [ -z "$region" ]
then
  echo "LockNamespace: region cannot be empty"
  exit 1
fi
if [ -z "$project" ]
then
  echo "LockNamespace: project cannot be empty"
  exit 1
fi
if [ -z "$namespace" ]
then
  echo "LockNamespace: namespace cannot be empty"
  exit 1
fi

printf "LockNamespace: Get credentials\n"
gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}
printf "LockNamespace: Check for secret\n"
if kubectl get secrets -n ${namespace} ${namespace}-inuse > /dev/null 2>&1; then
    printf "LockNamespace FAILED: Namepsace ${namespace} already in use.\n"
    exit 1
else
    printf "LockNamespace Namespace ${namespace} not in use, Running test runner on ${project}\n"
    kubectl create secret generic ${namespace}-inuse --from-literal=inuse=${namespace} -n ${namespace}
fi
