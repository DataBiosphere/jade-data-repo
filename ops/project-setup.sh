#!/bin/bash

PROJECT=broad-jade-integration
ZONE=us-east1-c

# make sure zone is set
gcloud config set compute/zone $ZONE

# set the project
gcloud config set project $PROJECT

###############################################################################
# Kubernetes commands
###############################################################################

gcloud iam service-accounts create jade-k8-sa --display-name 'Jade K8 SA'

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/bigquery.admin

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/cloudsql.admin

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/errorreporting.writer

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/logging.logWriter

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/monitoring.admin

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/servicemanagement.serviceController

gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com \
  --role roles/storage.admin

# for container registry the access is done on the bucket
gsutil iam ch \
  serviceAccount:jade-k8-sa@${PROJECT}.iam.gserviceaccount.com:objectViewer \
  gs://artifacts.broad-jade-dev.appspot.com

gcloud container clusters create k8-cluster \
  --enable-basic-auth \
  --issue-client-certificate \
  --metadata disable-legacy-endpoints=true \
  --enable-ip-alias \
  --num-nodes=1 \
  --disk-size=32GB \
  --machine-type=g1-small \
  --service-account=jade-k8-sa@${PROJECT}.iam.gserviceaccount.com

# put the secrets up there
kubectl create -f kubernetes/secrets.yaml

# create the deployment to map from secrets to env vars
kubectl create -f kubernetes/deployment.yaml

# create the service to explose port 8080 on port 80 of the cluster
kubectl create -f kubernetes/service.yaml

###############################################################################
# Database commands
###############################################################################
#
# !!! WARNING !!!
#
# We need to get a private IP for this database so that it will be in a VPC peered
# with the Kubernetes cluster. I cannot get the command below to work so the ONLY
# way for me to get the connection right was to do it from the Google Cloud UI.
#
# To use a private IP the docs say you should specify --no-assign-ip and use a
# private network, but there doesn't seem to be an option to specify a private network.
# I tried setting the authorized network to the CIDR block of the Kubernetes cluster
# but it still complains.
#
# --availability-type: zonal for non-prod, regional otherwise
# --no-backup: delete this option for production
# --maintenance-release-channel: preview for non-prod, production otherwise
# --tier: micro for test. we will use --cpu and --memory for production, and replicas
gcloud sql instances create repository-metadata \
  --database-version=POSTGRES_9_6 \
  --availability-type=zonal \
  --zone='us-east1-c' \
  --no-backup \
  --maintenance-release-channel=preview \
  --tier=db-f1-micro \
  --no-assign-ip \
  --authorized-networks=10.10.0.0/24

# After creating this in the UI, save the private IP address of the instance to use below.

gcloud sql users create username --instance=repository-metadata \
  --password=password
gcloud sql databases create stairway --instance=repository-metadata
gcloud sql databases create datarepo --instance=repository-metadata

# Now once the database is created, we need to enable extensions on it.
# First, get psql available in the cluster by spinning up a postgresql image:
kubectl run postgres-connector --image=postgres

# Next find the name of the pod that was created. There's probably an easier way
# to specify a name in the last command or pull this automatically, but this works:
kubectl get pods

# Use name of the pod and then the password of the postgres user when prompted below:
kubectl exec -it postgres-connector-55c6dbd969-zgpvh -- \
  psql -h 10.97.32.5 -U postgres -d datarepo -c 'create extension pgcrypto'
