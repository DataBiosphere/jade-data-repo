#!/bin/bash

set -e

if [[ $# -lt 7 ]]; then
    echo "usage $0 <sam-url> <datarepo-id> <datarepo-ops-group-name> <ops-admin-email> <stewards-group-name> <steward-admin-email> <auth-token>"
    exit 1
fi

SAM_URL=$1
DATAREPO_ID=$2
OPS_GROUP_NAME=$3
OPS_ADMIN_EMAIL=$4
STEWARDS_GROUP_NAME=$5
STEWARD_ADMIN_EMAIL=$6
AUTH_TOKEN=$7

# Create the ops group
curl -X POST --header 'Content-Length: 0' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${OPS_GROUP_NAME}" 
# Add the user as an admin of the ops group
curl -X PUT --header 'Content-Length: 0' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${OPS_GROUP_NAME}/admin/${OPS_ADMIN_EMAIL}" 

# Get the email address for the ops group
OPS_EMAIL=$(curl --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${OPS_GROUP_NAME}")

# Create the stewards group
curl -X POST --header 'Content-Length: 0' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${STEWARDS_GROUP_NAME}" 
# Add the user as an admin of the stewards group
curl -X PUT --header 'Content-Length: 0' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${STEWARDS_GROUP_NAME}/admin/${STEWARD_ADMIN_EMAIL}" 

# Get the email address for the steward group
STEWARDS_EMAIL=$(curl --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/groups/v1/${STEWARDS_GROUP_NAME}")

# Create the datarepo resource with the steward's and admin's group
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" "${SAM_URL}/api/resources/v1/datarepo" --data-binary '{ "resourceId": "'"${DATAREPO_ID}"'", "policies": { "admin" : { "memberEmails": [ '"${OPS_EMAIL}"' ], "actions":[], "roles": ["admin"] }, "steward" : { "memberEmails": [ '"${STEWARDS_EMAIL}"' ], "actions":[], "roles": ["steward"] }}, "authDomain": [] }'

