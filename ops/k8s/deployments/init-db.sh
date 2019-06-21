#!/bin/bash
set -e

export PGPASSWORD=${DB_DATAREPO_PASSWORD}; \
psql -h cloudsql-proxy-service.data-repo -U ${DB_DATAREPO_USERNAME} -d datarepo \
-c "CREATE EXTENSION IF NOT EXISTS pgcrypto; TRUNCATE study, study_table, study_column, \
study_relationship, asset_specification, asset_column, asset_relationship CASCADE;"
