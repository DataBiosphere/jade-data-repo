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

cat > $outfile <<EOF
{
  "type": "service_account",
  "project_id": "broad-jade-dev",
  "private_key_id": "7ec01cc1fb49293f082dc6eee2fa4f0479e2f305",
  "private_key": "-----BEGIN PRIVATE KEY-----\n$SERVICE_ACCOUNT_PK\n-----END PRIVATE KEY-----\n",
  "client_email": "travis-access@broad-jade-dev.iam.gserviceaccount.com",
  "client_id": "102180013040579229956",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/travis-access%40broad-jade-dev.iam.gserviceaccount.com"
}
EOF
