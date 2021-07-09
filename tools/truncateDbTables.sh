#!/usr/bin/env bash

# This script truncates all tables in a personal deployment. It is coded to never be able to run against dev.
# Pass the '-n <namespace>' option to choose which deployment to clear out.
# Pass the '-l' option to clear out your local database running on port 5432.

set -e

declare NAMESPACE=""
declare PORT=""
declare LOCAL=false
declare DEV_REGEX="^[dev]"
declare PROXY_COMMAND_PID=""

function finish {
  unset PGPASSWORD

  if ps -p "${PROXY_COMMAND_PID}" > /dev/null; then
    kill "${PROXY_COMMAND_PID}"
  fi
}

trap finish EXIT

function stderr {
    >&2 echo "$@"
}

function proxy_db_return_pid {
  gcloud container clusters get-credentials dev-master --region us-central1 --project broad-jade-dev
  local -r pod=$(kubectl get pod --namespace "${NAMESPACE}" | grep sqlproxy | awk '{print $1}')
  if [[ -z ${pod} ]]; then
    stderr "Could not find sqlproxy pod in ${NAMESPACE} namespace. Exiting."
    exit 1
  fi
  stderr "Connecting to pod ${pod}"
  kubectl port-forward --namespace "${NAMESPACE}" "${pod}" 5433:5432 > /dev/null &
  local pid=$!
  # Sleep for a little bit to allow for the port forwarding to take effect
  sleep 5
  echo "${pid}"
}

function truncate_tables {
  local schema_addendum=""
  if ! ${LOCAL}; then
    schema_addendum="-${NAMESPACE}"
  fi

  export PGPASSWORD=$(vault read -field datarepopassword secret/dsde/datarepo/dev/helm-datarepodb-dev)

  local -a datarepo_truncate_commands=()
  local table_retrieve="SELECT tablename FROM pg_catalog.pg_tables where schemaname = 'public' and tablename not like 'databasechangelog%'"
  local -a datarepo_tables=($(psql -h 127.0.0.1 -p "${PORT}" -t -U drmanager -d datarepo"${schema_addendum}" -c "${table_retrieve}"))
  for table in "${datarepo_tables[@]}"; do
    datarepo_truncate_commands+=("TRUNCATE TABLE ${table} CASCADE;")
  done

  local -a stairway_truncate_commands=()
  local -a stairway_tables=($(psql -h 127.0.0.1 -p "${PORT}" -t -U drmanager -d stairway"${schema_addendum}" -c "${table_retrieve}"))
  for table in "${stairway_tables[@]}"; do
    stairway_truncate_commands+=("TRUNCATE TABLE ${table} CASCADE;")
  done

  printf "%s\n" "${datarepo_truncate_commands[@]}" | psql -h 127.0.0.1 -p "${PORT}" -U drmanager -d datarepo"${schema_addendum}"

  printf "%s\n" "${stairway_truncate_commands[@]}" | psql -h 127.0.0.1 -p "${PORT}" -U drmanager -d stairway"${schema_addendum}"
}

function main {
    stderr "using namespace ${NAMESPACE}"
    if ! ${LOCAL}; then
      PROXY_COMMAND_PID=$(proxy_db_return_pid)
    fi

    truncate_tables

    if ! ${LOCAL}; then
      kill "${PROXY_COMMAND_PID}"
    fi

    stderr "Done clearing database in ${NAMESPACE:-local} deployment"
}


while getopts "n:l" arg; do
  case ${arg} in
    n)
      NAMESPACE="${OPTARG}"
      PORT=5433
      ;;
    l)
      PORT=5432
      LOCAL=true
  esac
done



if [[ -z ${NAMESPACE} ]] && ! ${LOCAL}; then
    stderr "Must provide a namespace using the '-n' option or use -l for local database"
    exit 1
fi

if [[ ${NAMESPACE} =~ ${DEV_REGEX} ]]; then
    stderr "This script cannot be used for the 'dev' namespace"
    exit 1
fi

main
