#!/bin/bash

clusterShortName=$1
region=$2
project=$3
namespace=$4

if [ -z "$clusterShortName" ]
then
  echo "UnlockNamespace: clusterShortName cannot be empty"
  exit 1
fi
if [ -z "$region" ]
then
  echo "UnlockNamespace: region cannot be empty"
  exit 1
fi
if [ -z "$project" ]
then
  echo "UnlockNamespace: project cannot be empty"
  exit 1
fi
if [ -z "$namespace" ]
then
  echo "UnlockNamespace: namespace cannot be empty"
  exit 1
fi
if [ -z "$id" ]
then
  echo "UnlockNamespace: deployment id cannot be empty"
  exit 1
fi

printf "UnlockNamespace: Get credentials\n"
gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}
printf "UnlockNamespace: Clear lock\n"
kubectl delete secret -n ${namespace} ${namespace}-inuse
kubectl delete secret -n ${namespace} ${namespace}-inuse-${id}
