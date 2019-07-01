#!/bin/bash
set -e

export PGPASSWORD=${DB_DATAREPO_PASSWORD}; \
psql -h cloudsql-proxy-service.data-repo -U ${DB_DATAREPO_USERNAME} -d datarepo \
-c "CREATE EXTENSION IF NOT EXISTS pgcrypto; TRUNCATE dataset, dataset_table, dataset_column, \
dataset_relationship, asset_specification, asset_column, asset_relationship CASCADE;"
