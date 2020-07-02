#!/bin/bash
# This script will eventually be a cleanup script to be used ahead of running integration tests on Jade data projects.
# For now, all it does is remove all of the SAM policies from the BigQuery Job User role.
# Require a project id
: ${1?"Usage: $0 projectid"}
projectid=$1
# get the policy bindings for the project
bindings=$(gcloud projects get-iam-policy $projectid --format=json)
# get the members of the BigQuery Job User role
members=$(echo $bindings | jq '.bindings[] | if .role == "roles/bigquery.jobUser" then .members else empty end')
# Loop through the members that start with "group:policy-" That is the signature of a SAM group. And I hope nothing else important!
# Remove the members one by one - this is noisy, but leaving it that way for now so we see the results in the log
for row in $(echo $members | jq -r '.[] | select(startswith("group:policy-"))'); do
    echo "removing member: ${row}"
    gcloud projects remove-iam-policy-binding $projectid --member=$row --role=roles/bigquery.jobUser > /dev/null
done
