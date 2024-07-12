#!/bin/sh
#
# This script is used to connect to the TDR Cloud SQL database in a GCP project.
#
# It uses the gcloud CLI to get the credentials for the cluster, kubectl to port
# forward to the SQL proxy pod, and psql to connect to the database.
#
# You MUST have gcloud, kubectl, jq, and psql installed to run this script. In
# addition, you MUST be connected to the VPN to access the database.
#
# Usage: ./db-connect.sh
#
# The following environment variables may be set to override the defaults:
#
#   PROJECT     the GCP project that contains the Cloud SQL database
#
#   NAMESPACE   the Kubernetes namespace for the SQL proxy pod
#               these can be listed with `kubectl get namespaces`
#
#   SECRET      the secret path for the database password in GCP Secrets Manager
#               the paths are listed at https://github.com/broadinstitute/vault-migration-tool/blob/main/mappings/tdr.yaml
#
#   PORT        the local port to forward to the SQL proxy pod
#
#   DATABASE    the name of the database to connect to
#

set -eu

# Default values that may be overridden by environment variables
PROJECT="${PROJECT:-broad-jade-dev}"
NAMESPACE="${NAMESPACE:-dev}"
SECRET="${SECRET:-helm-datarepodb}"
PORT="${PORT:-5432}"
DATABASE="${DATABASE:-stairway}"

error() {
    echo "ERROR: $1" >&2
    exit 1
}

cleanup() {
  kill "$PID"
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
  SQLPROXY_POD=$(kubectl get pods --namespace="$NAMESPACE" --output="json" | jq -r '[ .items[].metadata.name | select(. | contains("sqlproxy")) ].[0]')
  kubectl port-forward "$SQLPROXY_POD" --namespace "$NAMESPACE" "$PORT:5432" &
  PID=$!
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

trap cleanup EXIT

gcloud config set project "$PROJECT"

set_cluster_credentials
port_forward_sqlproxy
connect_cloud_sql_db
