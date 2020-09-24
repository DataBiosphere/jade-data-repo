#!/bin/bash
# This script will eventually be a cleanup script to be used ahead of running integration tests on Jade data projects.
# For now, all it does is remove all of the SAM policies from the BigQuery Job User role.
# Require a project id
: ${1?"Usage: $0 projectid"}
projectid=$1
# retrieve all IAM policies for data project
BINDINGS=$(gcloud projects get-iam-policy ${projectid} --format=json)
# remove any policies that start with group:policy- or deleted:group:policy-
# group policies are created as a part of our test run and need to be cleared out
# to avoid hitting 250 IAM policy limit
OK_BINDINGS=$(echo ${BINDINGS} | jq 'del(.bindings[] | select(.role=="roles/bigquery.jobUser") | .members[] | select(startswith("group:policy-") or startswith("deleted:group:policy-")))')
# replace the IAM policy, including only non-group policies/users
echo ${OK_BINDINGS} | jq '.' > policy.json
gcloud projects set-iam-policy ${projectid} policy.json