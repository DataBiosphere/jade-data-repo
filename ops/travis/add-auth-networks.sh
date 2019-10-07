#!/bin/bash

set -e

# This script needs to be run so that Travis can communicate with the integration kubernetes instance in order to
# deploy code to run tests against.

# usage: ./add-auth-networks.sh

# get the current authorized cidr blocks for the integration k8s master node
CURRENT=$(gcloud container clusters describe integration-us-central1-k8s --zone us-central1-a --format json | \
    jq -r '[.masterAuthorizedNetworksConfig.cidrBlocks[] | .cidrBlock]')

# fetch the latest Linux IP DNS A records from Travis and append /32 since it is just 1 IP
TRAVIS_IPS=$(curl -s https://dnsjson.com/nat.gce-us-central1.travisci.net/A.json | \
    jq '.results.records | map(.+"/32")')

# concatenate the arrays together, unique them, and join with a comma
NEW_IPS=$(printf '%s\n' $CURRENT $TRAVIS_IPS | jq -s -r 'add | unique | join(",")')

gcloud container clusters update integration-us-central1-k8s --zone us-central1-a --project broad-jade-integration \
    --enable-master-authorized-networks \
    --master-authorized-networks $NEW_IPS
