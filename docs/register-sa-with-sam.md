# Background

There is a Service Account (SA) that the repository runs under. In order for this SA to be able to be added to SAM
groups, for instance when files are shared with a SAM group that the repository needs to ingest, we need to register
the SA with SAM.

In order to do this, a user that is already registered with SAM must generate an access token for that SA and use it
to hit the user registration endpoint. This user must have both Service Account User AND Token Creator permissions
for the SA.

# Updating IAM Policies

First get the policy for the SA and write it to a file:

    gcloud iam service-accounts get-iam-policy <sa-name>@<project>.iam.gserviceaccount.com --format json > policy.json

It's possible that there are already policy bindings on this SA. In my case it wasn't, I just got an object with an
`etag` and `version` property. You'll want to update this object so that it has these two roles and the members that
will be able to impersonate/generate tokens for the SA.

    {
      "etag": "CAKE",
      "version": 1,
      "bindings": [
        {
          "role": "roles/iam.serviceAccountTokenCreator",
          "members": [
            "group:<group-name>@<domain.org>"
          ]
        },
        {
          "role": "roles/iam.serviceAccountUser",
          "members": [
            "user:<group-name>@<domain.org>"
          ]
        }
      ]
    }

It is important that the `etag` matches what you got from the previous step. The API will reject the request otherwise.

Next you'll want to set the new policy:

    gcloud iam service-accounts set-iam-policy <sa-name>@<project>.iam.gserviceaccount.com policy.json

Now, while authenticated as one of the service account users, you can generate an access token for the SA and use that
to register with SAM.


    #!/bin/bash

    get_token() {
      curl -sH "Authorization: Bearer $(gcloud auth print-access-token)" \
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/$1:generateAccessToken" \
        -H "Content-Type: application/json" \
        -d "{
          \"scope\": [
              \"https://www.googleapis.com/auth/userinfo.email\",
              \"https://www.googleapis.com/auth/userinfo.profile\"
          ]
        }" | jq -r .accessToken
    }

    curl https://sam.dsde-prod.broadinstitute.org/register/user/v1 -d "" \
        -H "Authorization: Bearer $(get_token <sa-name>@<project>.iam.gserviceaccount.com)"
