#!/bin/bash
#
# Try to add a member to the policy
# NOTE: this is meant to be source'd not executed

# -- main --

 : ${DRAPI:?}
 : ${AUTH_TOKEN:?}
 : ${PROFILE_ID:?}

echo "PROFILE_ID=$PROFILE_ID"
echo "DRAPI=$DRAPI"

ADD_BODY=$(cat <<EOF
{
  "email": "mcgonagall.curator@test.firecloud.org"
}
EOF
)

ADD_RESPONSE=$(echo ${ADD_BODY} | curl -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' \
                    -H "Authorization: Bearer ${AUTH_TOKEN}" -d @- \
                    ${DRAPI}/api/resources/v1/profiles/${PROFILE_ID}/policies/user/members)
echo "ADD_RESPONSE=$ADD_RESPONSE"
