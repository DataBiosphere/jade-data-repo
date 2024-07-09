#!/bin/sh
#
# Run Terra Data Repo locally with a Postgres database.
# You MUST have Docker and Vault installed to run this script.
#

# Login to Vault
vault login -method=github token="$(cat ~/.github-token)"

# Copy the secrets to the local environment
# shellcheck disable=SC1091
. ./render-configs.sh

# Ensure environment variables are defined
set -a
# shellcheck disable=SC1091
. .env
set +a

# Run Postgres database and set up the schema
docker run --rm \
  --name terra-data-repo-api \
  -v ./db:/docker-entrypoint-initdb.d \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
  -p 5432:5432 -d postgres:11

# Run Terra Data Repo
./gradlew clean assemble
./gradlew bootRun

# Cleanup Postgres database
docker stop terra-data-repo-api
