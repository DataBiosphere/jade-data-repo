#!/bin/bash
#
# Precondition: env var SERVICE_ACCOUNT_PK is set with the private key of the
# travis-access service account.
#
# This script generates the full credential file for GCP filling in the private
# key on th eway.
#
# $1 - output file
#

outfile=$1

if [ -z "$outfile" ]; then
    echo "ERROR: No output file specified"
    exit 1
fi

if [ -z "$SERVICE_ACCOUNT_PK" ]; then
    echo "ERROR: Environment variable SERVICE_ACCOUNT_PK is undefined"
    exit 1
fi

# The SERVICE_ACCOUNT_PK is the private key with prefix, suffix, and newlines removed.
# We add those back in here and then generate the result file
pk="-----BEGIN PRIVATE KEY-----\n"
p=0
l=64
str=$SERVICE_ACCOUNT_PK
while : ; do
    ss=${str:p:l}
    if [ -z "$ss" ]; then
        break
    fi
    pk="$pk$ss\n"
    p=$((p+l))
done
pk="$pk-----END PRIVATE KEY-----\n"

cat > $outfile <<EOF
{
  "type": "service_account",
  "project_id": "broad-jade-dev",
  "private_key_id": "7ec01cc1fb49293f082dc6eee2fa4f0479e2f305",
  "private_key": "$pk",
  "client_email": "travis-access@broad-jade-dev.iam.gserviceaccount.com",
  "client_id": "102180013040579229956",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/travis-access%40broad-jade-dev.iam.gserviceaccount.com"
}
EOF
