#!/bin/sh
#
# Connect to a Terra Data Repo database in an environment.
#
# It uses the gcloud CLI to get the credentials for the cluster, kubectl to port
# forward to the SQL proxy pod, and psql to connect to the database.
#
# You MUST have gcloud, kubectl, jq, and psql installed to run this script. In
# addition, you MUST be connected to the VPN to access the database.
#
# See usage section below for more details. All arguments are optional.
#

set -eu

usage() {
  cat <<EOF
Usage: $0 [OPTION]...
Connect to a Terra Data Repo database in an environment.

You MUST have gcloud, kubectl, jq, and psql installed to run this script. In
addition, you MUST be connected to the VPN to access the database.

  --env ENV             Environment to connect to, either dev or staging
                        (default: dev)
  --port PORT           Local port to forward to the SQL proxy (default: 5432)
  --database DATABASE   Database to connect to, either datarepo or stairway
                        (default: datarepo)
  --help                Display this help and exit
EOF
  exit 0
}

error() {
  echo "ERROR: $1" >&2
  exit 1
}

# default values that may be overridden by command line arguments or environment variables
ENV="${ENV:-dev}"
PORT="${PORT:-5432}"
DATABASE="${DATABASE:-datarepo}"

parse_cli_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --env)
        ENV="$2"
        shift 2
        ;;
      --port)
        PORT="$2"
        shift 2
        ;;
      --database)
        if [ "$2" != "datarepo" ] && [ "$2" != "stairway" ]; then
          error "Database must be one of 'datarepo' or 'stairway'"
        fi
        DATABASE="$2"
        shift 2
        ;;
      --help)
        usage
        ;;
      *)
        error "Unknown option: $1. Try --help to see a list of all options."
        ;;
    esac
  done
}

cleanup() {
  kill "$PID"
}

set_vars_from_env() {
  case "$ENV" in
    dev)
      PROJECT="broad-jade-dev"
      NAMESPACE="dev"
      SECRET="helm-datarepodb"
      ;;
    staging)
      PROJECT="terra-datarepo-staging"
      NAMESPACE="terra-staging"
      SECRET="sql-db"
      ;;
    *)
      error "Unknown environment: $ENV"
      ;;
  esac
}

set_project_config() {
  gcloud config set project "$PROJECT"
}

set_cluster_credentials() {
  CLUSTER_JSON=$(gcloud container clusters list --format="json")

  REGION=$(echo "$CLUSTER_JSON" | jq -r .[0].zone)
  NAME=$(echo "$CLUSTER_JSON" | jq -r .[0].name)

  gcloud container clusters get-credentials "$NAME" --region="$REGION" --project="$PROJECT"
}

port_forward_sqlproxy() {
  POD_JSON=$(kubectl get pods --namespace="$NAMESPACE" --output="json")

  # validate that the namespace is in the list of namespaces
  if ! echo "$POD_JSON" | jq -r '.items[].metadata.namespace' | grep -q "$NAMESPACE"; then
    error "Namespace '$NAMESPACE' not found in list of namespaces"
  fi

  # select the first pod that has a name that contains "sqlproxy"
  SQLPROXY_POD=$(kubectl get pods --namespace="$NAMESPACE" --output="json" | jq -r '.items | map(.metadata.name | select(contains("sqlproxy"))) | first')
  kubectl port-forward "$SQLPROXY_POD" --namespace "$NAMESPACE" "$PORT:5432" &
  PID=$!
  trap cleanup EXIT
}

connect_cloud_sql_db() {
  USERNAME="drmanager"
  PASSWORD=$(gcloud secrets versions access latest --project="$PROJECT" --secret="$SECRET" | jq -r '.datarepopassword')

  # validate that the password is not empty
  if [ -z "$PASSWORD" ]; then
    error "Could not retrieve password for project '$PROJECT' with secret path '$SECRET'"
  fi

  psql "postgresql://$USERNAME:$PASSWORD@localhost:$PORT/$DATABASE"
}

parse_cli_args "$@"
set_vars_from_env
set_project_config
set_cluster_credentials
port_forward_sqlproxy
connect_cloud_sql_db
