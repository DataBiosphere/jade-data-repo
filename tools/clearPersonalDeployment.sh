#!/usr/bin/env bash

# This script truncates all tables in a personal deployment. It is coded to never be able to run against dev.
# Pass the '-n <namespace>' option to choose which deployment to clear out.

declare NAMESPACE=""
declare dev_regex="^[dev]"
declare -a TABLES_TO_TRUNCATE=(databasechangeloglock
databasechangelog
dataset
dataset_column
snapshot_table
snapshot
asset_column
asset_relationship
dataset_table
snapshot_column
snapshot_source
snapshot_map_table
dataset_relationship
asset_specification
snapshot_map_column
project_resource
billing_profile
bucket_resource
load
load_file
snapshot_relationship
dataset_bucket
storage_resource)

function finish {
  unset PGPASSWORD
}
trap finish EXIT

function stderr {
    >&2 echo "$@"
}

function proxy_db_return_pid {
  local pod=$(kubectl get pod --namespace ${NAMESPACE} | grep sqlproxy | awk '{print $1}')
  stderr "Connecting to pod ${pod}"
  kubectl port-forward --namespace tl ${pod} 5433:5432 > /dev/null &
  echo $!
}

function truncate_tables {

  local -a truncate_commands=()
  for table in ${TABLES_TO_TRUNCATE[@]}; do
    truncate_commands+=("TRUNCATE TABLE ${table} CASCADE;")
  done

  export PGPASSWORD=$(vault read -field datarepopassword secret/dsde/datarepo/dev/helm-datarepodb-dev)
  printf "%s\n" "${truncate_commands[@]}" | psql -h 127.0.0.1 -p 5433 -U drmanager -d datarepo-${NAMESPACE}
}

function main {
    stderr "using namespace ${NAMESPACE}"
    local proxy_command_pid=$(proxy_db_return_pid)
    truncate_tables
    kill ${proxy_command_pid}
    stderr "Done clearing database in ${NAMESPACE} deployment"
}


while getopts "n:" arg; do
  case ${arg} in
    n)
      NAMESPACE=${OPTARG}
      ;;
  esac
done

if [[ -z ${NAMESPACE} ]]; then
    stderr "Must provide a namespace using the '-n' option"
    exit
fi

if [[ ${NAMESPACE} =~ ${dev_regex} ]]; then
    stderr "This script cannot be used for the 'dev' namespace"
    exit
fi

main
