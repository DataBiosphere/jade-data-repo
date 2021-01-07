#!/bin/bash
#
# Try to delete the profile
# NOTE: this is meant to be source'd not executed

# -- main --

 : ${DRAPI:?}
 : ${AUTH_TOKEN:?}
 : ${PROFILE_ID:?}

echo "PROFILE_ID=$PROFILE_ID"
echo "DRAPI=$DRAPI"

PROFILE_JOB=$(curl -X DELETE -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  ${DRAPI}/api/resources/v1/profiles/${PROFILE_ID})
echo "PROFILE_JOB=$PROFILE_JOB"
JOBID=$(echo $PROFILE_JOB | jq -r ".id")
echo "JOBID=$JOBID"

RESULT=$(waitForComplete ${JOBID})
echo "RESULT=$RESULT"
echo "Deleted billing profile ${PROFILE_ID}"
