#!/bin/bash

clusterShortName=$1
region=$2
project=$3
namespace=$4

if [ -z "$clusterShortName" ]
then
  echo "clusterShortName cannot be empty"
  exit 1
fi
if [ -z "$region" ]
then
  echo "region cannot be empty"
  exit 1
fi
if [ -z "$project" ]
then
  echo "project cannot be empty"
  exit 1
fi
if [ -z "$namespace" ]
then
  echo "namespace cannot be empty"
  exit 1
fi

printf "LOCKENV: Get credentials\n"
gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}
printf "LOCKENV: Check for secret\n"
if kubectl get secrets -n ${namespace} ${namespace}-inuse > /dev/null 2>&1; then
    printf "LOCKENV FAILED: Namepsace ${namespace} already in use.\n"
    exit 1
else
    printf "LOCKENV: Namespace ${namespace} not in use, Running test runner on ${project}\n"
    kubectl create secret generic ${namespace}-inuse --from-literal=inuse=${namespace} -n ${namespace}
fi
