#!/bin/bash
# NOTE: this is meant to be source'd not executed. The functions and env vars are needed by later scripts
# You must have your gcloud configured to the correct account.
#
export AUTH_TOKEN=$(gcloud auth print-access-token)
export DRAPI="https://jade-dd.datarepo-dev.broadinstitute.org"

# -- functions --

function waitForComplete() {
    JOBID=$1

    POLL_STATUS_CODE=$(poll ${JOBID})
    while [ "$POLL_STATUS_CODE" != "200" ]; do
        sleep 5
        POLL_STATUS_CODE=$(poll ${JOBID})
    done

     RESULT=$(curl -H 'Content-Type: application/json' -H 'Accept: application/json' \
                  -H "Authorization: Bearer ${AUTH_TOKEN}" \
                  ${DRAPI}/api/repository/v1/jobs/${JOBID}/result)
    echo $RESULT
}

function poll() {
    JOBID=$1
    POLL_RESPONSE=$(curl -H 'Content-Type: application/json' -H 'Accept: application/json' \
                             -H "Authorization: Bearer ${AUTH_TOKEN}" \
                             -s -o /dev/null -w "%{http_code}" \
                             ${DRAPI}/api/repository/v1/jobs/${JOBID})
    echo "${POLL_RESPONSE}"
}

