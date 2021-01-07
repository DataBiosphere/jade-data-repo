#!/bin/bash
#
# Do this script before the upgrade release is installed
# NOTE: this is meant to be source'd not executed - as output it sets PROFILE_ID env var

# -- main --

 : ${DRAPI:?}
 : ${AUTH_TOKEN:?}

# Create the canary billing account. It will not have a Sam resource
export PROFILE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
echo "ProfileId=$PROFILE_ID"
BILLING_ACCOUNT_ID="00708C-45D19D-27AAFA"

PROFILE_BODY=$(cat <<EOF
{"id":"${PROFILE_ID}",
 "billingAccountId":"${BILLING_ACCOUNT_ID}",
 "profileName":"canaryProfile",
 "biller":"direct",
 "description":"canary profile"
}
EOF
)

PROFILE_JOB=$(echo $PROFILE_BODY | curl -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH_TOKEN}" -d @- \
  ${DRAPI}/api/resources/v1/profiles)
echo "PROFILE_JOB=$PROFILE_JOB"
JOBID=$(echo $PROFILE_JOB | jq -r ".id")
echo "JOBID=$JOBID"

RESULT=$(waitForComplete ${JOBID})
echo "RESULT=$RESULT"
PROFILENAME=$(echo $RESULT | jq -r ".profileName")
echo "Created billing profile ${profilename}"
