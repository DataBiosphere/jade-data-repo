#!/bin/bash
# This script is also used as a cleanup script used ahead of manual cleanup of the broad-jade-dev-data project. 
# SAM policies need to be cleared out to avoid hitting 250 IAM policy limit

#---USAGE---
#gcloud auth login #your dev gmail
#export token = $(gcloud auth print-access-token)
#gcloud auth login #your broad email
#./dev-data-cleanup.sh broad-jade-dev-data

#----RESULT---
#This generates a few files, including:
#1. iamPolicyONLY.txt. These are the policies that should be removed.
#2. backupPolicy.json - existing policy
#NEXT STEPS:
#1. Make a copy of backPolicy.json, named updatedPolicy.json
#2. Remove group policies listed in iamPolicyONLY.txt
#3. Update the IAM policy to use the udpated one by running:
# gcloud projects set-iam-policy broad-jade-dev-data updatedPolicy.json

# Require a project id
: ${1?"Usage: $0 PROJECT_ID"}
PROJECT_ID=$1

PROJECT_EXISTS=$(gcloud projects list --filter ${PROJECT_ID} --uri 2>/dev/null)
if [ -z "${PROJECT_EXISTS}" ]; then
    echo "ERROR: Cannot find project '${PROJECT_ID}' (do you have permission?)"
    exit 1
fi

#Require user to set BEARER TOKEN
if [ -z "${token}" ]; then
    echo "ERROR set token env variable. Try gcloud auth login and then export token=$(gcloud auth print-access-token)"
    exit 1
fi

FOLDER_NAME=dev-data-cleanup
mkdir ${FOLDER_NAME}

#Start by creating backup
BINDINGS=$(gcloud projects get-iam-policy ${PROJECT_ID} --format=json)
echo ${BINDINGS} | jq '.' > ${FOLDER_NAME}/backupPolicy.json

echo "Collecting policies attached to either datasets or snapshots"
DATASET_POLICIES=$(curl -s -X GET "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/datasets?direction=desc&limit=10000&offset=0&sort=created_date" -H "accept: application/json" -H "authorization: Bearer ${token}" | jq -r .items[].id | xargs -I datasetId curl -s -X GET "https://sam.dsde-dev.broadinstitute.org/api/resources/v2/dataset/datasetId/policies" -H "accept: application/json" -H "authorization: Bearer ${token}" | jq -r .[].email)
echo $DATASET_POLICIES >> ${FOLDER_NAME}/samPolicies.txt
SNAPSHOT_POLICIES=$(curl -s -X GET "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/snapshots?direction=desc&limit=10000&offset=0&sort=created_date" -H "accept: application/json" -H "authorization: Bearer ${token}" | jq -r .items[].id | xargs -I snapshotId curl -s -X GET "https://sam.dsde-dev.broadinstitute.org/api/resources/v2/datasnapshot/snapshotId/policies" -H "accept: application/json" -H "authorization: Bearer ${token}" | jq -r .[].email)
echo $SNAPSHOT_POLICIES >> ${FOLDER_NAME}/samPolicies.txt
cat ${FOLDER_NAME}/samPolicies.txt | tr ' ' '\n' | sort -u > ${FOLDER_NAME}/samPoliciesSorted.txt

echo "Retrieve all BQ Group IAM policies for gcloud project"
BQ_BINDINGS=$(echo ${BINDINGS} | jq '.bindings[] | select(.role=="roles/bigquery.jobUser") | .members[] | select(startswith("group:policy-"))')
echo $BQ_BINDINGS > ${FOLDER_NAME}/iamPolicies.txt
cat ${FOLDER_NAME}/iamPolicies.txt | tr ' ' '\n' | xargs -n1 | cut -d ':' -f2 | sort -u > ${FOLDER_NAME}/iamPoliciesSorted.txt

echo "Comparing the two sorted files"
comm -13 ${FOLDER_NAME}/samPoliciesSorted.txt ${FOLDER_NAME}/iamPoliciesSorted.txt > ${FOLDER_NAME}/iamPolicyONLY.txt

echo "DONE"
echo "Check $FOLDER_NAME for results. Policies that can be removed from the google project should be listed in iamPolicyONLY.txt"
