#!/bin/bash

clusterShortName=$1
region=$2
project=$3

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

printf "LOCKENV: Get credentials\n"
gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}
printf "LOCKENV: Check for secret\n"
if kubectl get secrets -n ${project} ${project}-inuse > /dev/null 2>&1; then
    printf "LOCKENV: Namespace ${project} in use Skipping\n"
    exit 2
else
    printf "LOCKENV: Namespace ${project} not in use, Running test runner on ${project}\n"
    kubectl create secret generic ${project}-inuse --from-literal=inuse=${project} -n ${project}
    printf "LOCKENV: Successfully created secret.\n"
fi
