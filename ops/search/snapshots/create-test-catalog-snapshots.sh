#!/usr/bin/env bash

# Modified from https://github.com/broadinstitute/dsp-scripts/blob/master/firecloud/create-snapshot.sh
# Example usage:
# bash create-snapshot.sh $(gcloud auth print-access-token "<user@email.com>") < dev-catalog-inputs.txt
#
# See:
# - https://github.com/DataBiosphere/jade-data-repo/tree/develop/src/test/resources
# - https://console.cloud.google.com/storage/browser/jade-testdata/ingest-test
# - https://console.cloud.google.com/storage/browser/000-dvoet-test

hash jq 2>/dev/null || {
  echo >&2 "This script requires jq, but it's not installed. On Darwin, just \"brew install jq\". Aborting."
  exit 1
}

remove_quotes() {
  tr -d "\""
}

collapse_lines() {
  tr "\n" " "
}

echo "This script will create a snapshot for a given user in Data Repo on a non-prod environment."
echo "If you want to use an auth token for a user you're not gcloud auth-ed as, cancel and call this script with the token as an argument."
echo

if [ -n "$1" ]; then
  echo "Reading bearer token from argument."
  BEARER_TOKEN=$1
else
  echo "gcloud is authorized with the following accounts:"
  read -r -a LIST <<<"$(gcloud auth list --format json | jq '.[].account' | remove_quotes | collapse_lines)"
  PS3="❓ Pick an account to create a snapshot: "
  select ACCOUNT in "${LIST[@]}"; do
    echo "Getting a token for $ACCOUNT..."
    break
  done

  BEARER_TOKEN=$(gcloud auth print-access-token "$ACCOUNT" --quiet)
fi
echo


echo "This script supports the following environments:"
PS3="❓ Data repo environment: "
select ENV in dev alpha perf staging production; do
  case $ENV in
  dev)
    TDR_URL="https://jade.datarepo-dev.broadinstitute.org"
    ;;
  alpha)
    TDR_URL="https://data.alpha.envs-terra.bio"
    ;;
  perf)
    TDR_URL="https://jade-perf.datarepo-perf.broadinstitute.org"
    ;;
  staging)
    TDR_URL="https://data.staging.envs-terra.bio"
    ;;
  production)
    TDR_URL="https://data.terra.bio"
    ;;
  esac
  echo "Will call $TDR_URL"
  break
done
echo

# TDR curl helpers
tdr_post() { # args: path, post body
  echo "📡 Posting to $1..."
  CURL_RESPONSE=$(curl -w ";%{http_code}" -sX POST "$TDR_URL/api/$1" -H "accept: application/json" -H "authorization: Bearer $BEARER_TOKEN" -H "Content-Type: application/json" -d "$2")
  IFS=";" read -r CURL_OUTPUT CURL_RESPONSE_STATUS <<<"$(echo "$CURL_RESPONSE" | collapse_lines)"
  if [ $CURL_RESPONSE_STATUS -ge 300 ]; then
    echo
    echo "😕 Post failed! Output:"
    echo "$CURL_OUTPUT"
    exit 1
  fi
}

tdr_get() { # args: path
  echo "📡 Getting from $1..."
  CURL_RESPONSE=$(curl -w ";%{http_code}" -sX GET "$TDR_URL/api/$1" -H "accept: application/json" -H "authorization: Bearer $BEARER_TOKEN")
  IFS=";" read -r CURL_OUTPUT CURL_RESPONSE_STATUS <<<"$(echo "$CURL_RESPONSE" | collapse_lines)"
  if [ $CURL_RESPONSE_STATUS -ge 300 ]; then
    echo
    echo "😕 Get failed! Output:"
    echo "$CURL_OUTPUT"
    exit 1
  fi
}

tdr_poll_job() {
  CURRENT_JOB_ID=$(echo "$CURL_OUTPUT" | jq .id | remove_quotes)
  tdr_get "repository/v1/jobs/$CURRENT_JOB_ID"
  while [ "$(echo "$CURL_OUTPUT" | jq .job_status | remove_quotes)" == "running" ]; do
    echo "💤 Not done, checking again in 5 seconds"
    sleep 5
    tdr_get "repository/v1/jobs/$CURRENT_JOB_ID"
  done

  if [ "$(echo "$CURL_OUTPUT" | jq .job_status | remove_quotes)" == "failed" ]; then
    echo
    echo "😕 Job failed! Output:"
    tdr_get "repository/v1/jobs/$CURRENT_JOB_ID/result"
    echo "$CURL_OUTPUT" | jq .
    exit 1
  fi
}

read -rp "Number of dataset/snapshot pairs to create: " COUNT

if [ $ENV == "dev" ]; then
  echo "Using dev shared billing profile"
  BILLING_PROFILE_UUID="390e7a85-d47f-4531-b612-165fc977d3bd"
  BILLING_PROFILE_NAME="default"
  echo
  echo "⚠️️  Just so you know, dev Data Repo is a bit special. There's one shared billing profile for all users."
  echo "There's a Terra group called $(tput bold)Data-Repo-Integration-Testers$(tput sgr0) (which is a member of $(tput bold)JadeStewards-dev$(tput sgr0))"
  echo "that you'll want to make sure $ACCOUNT is a member of, so it has access. If you have issues,"
  echo "try one of the shared testing accounts, like b.adm.firec of a Testerson account, which have permission."
else
  echo "Creating a billing profile"
  BILLING_PROFILE_UUID=$(uuidgen | awk '{print tolower($0)}')
  echo "🎲 Random uuid: $BILLING_PROFILE_UUID"
  read -rp "❓ Need a display name for the profile: " BILLING_PROFILE_NAME
  tdr_post "resources/v1/profiles" "{\"id\":\"$BILLING_PROFILE_UUID\",\"billingAccountId\":\"01A82E-CA8A14-367457\",\"profileName\":\"$BILLING_PROFILE_NAME\",\"biller\":\"direct\",\"description\":\"Created by create-snapshot.sh\"}"

  echo
  echo "Verifying that billing profile was created"
  tdr_poll_job
fi
echo

read -rp "❓ Need a display name for the dataset: " DATASET_BASE_NAME
read -rp "❓ Need a display name for the snapshot: " SNAPSHOT_BASE_NAME

for i in $(seq 1 $COUNT); do
  echo "Creating dataset with billing profile $BILLING_PROFILE_NAME"
  DATASET_NAME="${DATASET_BASE_NAME}${i}"
  tdr_post "repository/v1/datasets" "{\"name\":\"$DATASET_NAME\",\"description\":\"Created by create-snapshot.sh\",\"defaultProfileId\":\"$BILLING_PROFILE_UUID\",\"schema\":{\"tables\":[{\"name\":\"participant\",\"columns\":[{\"name\":\"participant_id\",\"datatype\":\"string\"},{\"name\":\"sex\",\"datatype\":\"string\"},{\"name\":\"age\",\"datatype\":\"integer\"}]},{\"name\":\"sample\",\"columns\":[{\"name\":\"sample_id\",\"datatype\":\"string\"},{\"name\":\"participant_id\",\"datatype\":\"string\"},{\"name\":\"files\",\"datatype\":\"integer\",\"array_of\":true},{\"name\":\"type\",\"datatype\":\"string\"}]}],\"relationships\":[{\"name\":\"participant_sample\",\"from\":{\"table\":\"participant\",\"column\":\"participant_id\"},\"to\":{\"table\":\"sample\",\"column\":\"participant_id\"}}],\"assets\":[{\"name\":\"sample\",\"rootTable\":\"sample\",\"rootColumn\":\"sample_id\",\"tables\":[{\"name\":\"sample\",\"columns\":[]},{\"name\":\"participant\",\"columns\":[]}],\"follow\":[\"participant_sample\"]}]}}"
  tdr_poll_job
  echo

  tdr_get "repository/v1/jobs/$CURRENT_JOB_ID/result"
  DATASET_ID=$(echo "$CURL_OUTPUT" | jq .id | remove_quotes)
  echo "Dataset created with id $DATASET_ID"
  echo

  echo "Populating dataset $DATASET_NAME"
  tdr_post "repository/v1/datasets/$DATASET_ID/ingest" "{\"format\":\"json\",\"ignore_unknown_values\":true,\"max_bad_records\":0,\"path\":\"gs:\/\/jade-testdata\/data-catalog-testdata\/participants.json\",\"table\":\"participant\"}"
  echo "Waiting for ingest 1/2 to complete..."
  tdr_poll_job
  tdr_post "repository/v1/datasets/$DATASET_ID/ingest" "{\"format\":\"json\",\"ignore_unknown_values\":true,\"max_bad_records\":0,\"path\":\"gs:\/\/jade-testdata\/data-catalog-testdata\/samples.json\",\"table\":\"sample\"}"
  echo "Waiting for ingest 2/2 to complete"
  tdr_poll_job
  echo

  $SNAPSHOT_NAME="${SNAPSHOT_BASE_NAME}${i}"
  tdr_post "repository/v1/snapshots" "{\"name\":\"$SNAPSHOT_NAME\",\"description\":\"Created by create-snapshot.sh\",\"contents\":[{\"datasetName\":\"$DATASET_NAME\",\"mode\":\"byFullView\"}],\"profileId\":\"$BILLING_PROFILE_UUID\"}"
  echo "Waiting for snapshot creation"
  tdr_poll_job
  tdr_get "repository/v1/jobs/$CURRENT_JOB_ID/result"
  SNAPSHOT_ID=$(echo "$CURL_OUTPUT" | jq .id | remove_quotes)
  echo

  echo "Created public Snapshot $SNAPSHOT_NAME with id:"
  echo "$(tput bold)$SNAPSHOT_ID$(tput sgr0)"

  if (($i % 2 == 0)); then
    POLICY_URL="repository/v1/snapshots/$SNAPSHOT_ID/policies/discoverer/members"
  else
    POLICY_URL="repository/v1/snapshots/$SNAPSHOT_ID/policies/steward/members"
  fi
  echo "Updating permissions"
  # TODO: change to test service accounts
  tdr_post $POLICY_URL "{\"email\": \"voldemort.admin@test.firecloud.org\"}"

  echo "Done!"
done
