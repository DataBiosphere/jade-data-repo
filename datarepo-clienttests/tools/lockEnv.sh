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

gcloud container clusters get-credentials ${clusterShortName} --region ${region} --project ${project}

if kubectl get secrets -n ${project} ${project}-inuse > /dev/null 2>&1; then
    printf "Namespace ${project} in use Skipping\n"
    exit 2
else
    printf "Namespace ${project} not in use, Running test runner on ${project}\n"
    kubectl create secret generic ${project}-inuse --from-literal=inuse=${project} -n ${project}
    printf "Successfully created secret.\n"
fi
