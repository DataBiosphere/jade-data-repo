#!/bin/bash

# TODO: take cluster, region, project as required args

originalConfigVal=$(gcloud config get-value container/use_application_default_credentials)
gcloud config set container/use_application_default_credentials True
gcloud config get-value container/use_application_default_credentials

gcloud container clusters get-credentials dev-master --region us-central1 --project broad-jade-dev

gcloud config set container/use_application_default_credentials $originalConfigVal
gcloud config get-value container/use_application_default_credentials
cat $HOME/.kube/config
