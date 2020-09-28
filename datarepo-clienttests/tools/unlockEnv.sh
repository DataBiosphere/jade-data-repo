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

printf "UNLOCKENV: Get credentials\n"
gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}
printf "UNLOCKENV: Clear lock\n"
kubectl delete secret -n ${namespace} ${namespace}-inuse
