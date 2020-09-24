#!/bin/bash
# This script is also used as a cleanup script used ahead of running integration tests on Jade data projects.

# SAM policies are created as a part of our test run and need to be cleared out
# to avoid hitting 250 IAM policy limit

# Require a project id
: ${1?"Usage: $0 projectid"}
projectid=$1

# retrieve all IAM policies for gcloud project
BINDINGS=$(gcloud projects get-iam-policy ${projectid} --format=json)

#${BINDINGS} returns something in this format:
# {
#  "bindings": [
#    {
#      "members": [
#        "deleted:group:policy-0512f280-6ae8-45ec-877c-b25746d65866@dev.test.firecloud.org?uid=507418924967946102347",
#        "deleted:group:policy-116a1ea6-cd0b-4ab6-8e49-09e15c89b796@dev.test.firecloud.org?uid=518340755623420602811",
#        "group:JadeStewards-dev@dev.test.firecloud.org",
#        "group:JadeStewards-perf@dev.test.firecloud.org",
#      ],
#      "role": "roles/bigquery.jobUser"
#    }
#    ... lists of members for other roles...
#  ],
#  "etag": "BwWwDj_0y8I=",
#  "version": 1
# }


# remove any policies for user role BigQuery.JobUsers that start with group:policy- or deleted:group:policy-
OK_BINDINGS=$(echo ${BINDINGS} | jq 'del(.bindings[] | select(.role=="roles/bigquery.jobUser") | .members[] | select(startswith("group:policy-") or startswith("deleted:group:policy-")))')

# {OK_BINDINGS} traverses the json output from ${BINDINGS} selecting members to be deleted from policy
# [from "bindings" array, select member list for role bigquery.jobUser, select only group group policy members]
# After del, leaves us only with bindings we want to keep (e.g. group:JadeStewards-dev@dev.test.firecloud.org)

# replace the IAM policy, including only members selected in ${OK_BINDINGS}
echo ${OK_BINDINGS} | jq '.' > policy.json
gcloud projects set-iam-policy ${projectid} policy.json