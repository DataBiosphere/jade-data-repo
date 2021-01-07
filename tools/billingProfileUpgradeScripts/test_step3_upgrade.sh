#!/bin/bash
#
# Invoke the upgrade endpoint
# NOTE: this is meant to be source'd not executed

# -- main --

 : ${DRAPI:?}
 : ${AUTH_TOKEN:?}
 : ${PROFILE_ID:?}

echo "PROFILE_ID=$PROFILE_ID"
echo "DRAPI=$DRAPI"

UPGRADE_BODY=$(cat <<EOF
{
  "customArgs": [],
  "customName": "BILLING_PROFILE_PERMISSION",
  "upgradeName": "dd upgrade",
  "upgradeType": "custom"
}
EOF
)

UPGRADE_JOB=$(echo $UPGRADE_BODY | curl -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' \
  -H "Authorization: Bearer ${AUTH_TOKEN}" -d @- \
  ${DRAPI}/api/repository/v1/upgrade)
echo "UPGRADE_JOB=$UPGRADE_JOB"
JOBID=$(echo $UPGRADE_JOB | jq -r ".id")
echo "JOBID=$JOBID"

RESULT=$(waitForComplete ${JOBID})
echo "RESULT=$RESULT"

