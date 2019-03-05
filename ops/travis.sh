#!/bin/bash

# These were the steps I took to create a Travis Service Account that we can
# use during TravisCI jobs. This script needs to be gone through manuallhy as
# it is right now.

# create a service account for Travis to use
gcloud iam service-accounts create travis-access --display-name 'TravisCI SA'

# get the key in json form and escape it
cat /tmp/creds.json | \
  sed 's/ /\\ /g' | \
  sed 's/{/\\{/g' | \
  sed 's/"/\\"/g' | \
  sed 's/}/\\}/g' > /tmp/escaped-creds.json

# paste the escaped credentials into travis as a secure variable

gsutil iam ch \
  serviceAccount:travis-access@broad-jade-dev.iam.gserviceaccount.com:objectAdmin \
  gs://artifacts.broad-jade-dev.appspot.com

# add roles to the service account
gcloud projects add-iam-policy-binding broad-jade-integration \
  --member serviceAccount:travis-access@broad-jade-dev.iam.gserviceaccount.com \
  --role roles/container.developer
