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

originalConfigVal=$(gcloud config get-value container/use_application_default_credentials)
gcloud config set container/use_application_default_credentials True
gcloud config get-value container/use_application_default_credentials

gcloud container clusters get-credentials $clusterShortName --region $region --project $project

gcloud config set container/use_application_default_credentials $originalConfigVal
gcloud config get-value container/use_application_default_credentials
echo "$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"

echo "jade-data-repo fetch GKE Credentials"
echo "$(ls -all $HOME)"

cat 
cat $HOME/.kube/config
