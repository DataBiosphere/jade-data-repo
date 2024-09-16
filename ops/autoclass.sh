#!/bin/bash
#
# Applies the autoclass policy to a list of buckets read from a file. The file
# containing the list of bucket names must have one bucket name per line.
#
# See usage section below for more details.
#

set -eu

usage() {
  cat <<EOF
Usage: $0 [OPTION]... --file FILE
Applies the autoclass policy to a list of buckets. FILE is required.

You MUST have gcloud and jq installed and be authenticated as a Terra Data Repo
admin to run this script.

  --file FILE   The file containing the list of bucket names to apply the policy
                to, with one bucket name per line (REQUIRED)
  --env ENV     Terra Data Repo environment to use to apply the policy. Must be
                one of dev, staging, or prod. (default: dev)
  --help        Display this help and exit.
EOF
  exit 0
}

error() {
  echo "Error: $1" >&2
  exit 1
}

# default values that may be overridden by command line options
BUCKET_FILE="${BUCKET_FILE:-}"
TDR_ENV="${TDR_ENV:-dev}"
TDR_URL="${TDR_URL:-}"

check_tdr_env() {
  case "$1" in
    dev)
      TDR_URL="https://jade.datarepo-dev.broadinstitute.org"
      ;;
    staging)
      TDR_URL="https://data.staging.envs-terra.bio"
      ;;
    prod)
      TDR_URL="https://data.terra.bio"
      ;;
    *)
      error "Invalid environment: $1. Must be one of dev, staging, or prod."
      ;;
  esac
  TDR_ENV="$1"
}

parse_cli_args() {
  if [ $# -eq 0 ]; then
    usage
  fi
  while [ $# -gt 0 ]; do
    case "$1" in
      --file)
        BUCKET_FILE="$2"
        shift 2
        ;;
      --env)
        check_tdr_env "$2"
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
  if [ -z "$BUCKET_FILE" ]; then
    error "Missing required option: --file. Try --help to see a list of all options."
  fi
  if [ -z "$TDR_URL" ]; then
    check_tdr_env "$TDR_ENV"
  fi
}

curl_post() {
    curl --silent --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" --data "$2" "$1"
}

curl_get() {
    curl --silent --header 'Content-Type: application/json' --header 'Accept: application/json' --header "Authorization: Bearer ${AUTH_TOKEN}" "$1"
}

autoclass_submit_requests() {
  USER_INITIALS=$(gcloud config list account --format "value(core.account)" | head -c2)
  CUR_DATE=$(date +%Y%m%d)
  AUTOCLASS_REQUEST=$(cat << EOF
{
  "upgradeName": "${USER_INITIALS}_bucket_upgrade_${CUR_DATE}",
  "upgradeType": "custom",
  "customName": "SET_BUCKET_AUTOCLASS_ARCHIVE",
  "customArgs": [
    "__bucket_name__"
  ]
}
EOF
)
  while IFS= read -r BUCKET; do
    AUTOCLASS_REQUEST=$(echo "$AUTOCLASS_REQUEST" | jq --arg bucket "$BUCKET" '.customArgs[0] = $bucket')
    AUTOCLASS_JOB_ID=$(curl_post "${TDR_URL}/api/repository/v1/upgrade" "$AUTOCLASS_REQUEST" | jq -r .id)
    AUTOCLASS_JOBS+=("$AUTOCLASS_JOB_ID")
  done < "$BUCKET_FILE"
  printf "%s\n" "${AUTOCLASS_JOBS[@]}" > jobs.txt
}

autoclass_check_responses() {
  for JOB_ID in "${AUTOCLASS_JOBS[@]}"; do
    while true; do
      AUTOCLASS_STATUS=$(curl_get "${TDR_URL}/api/repository/v1/jobs/${JOB_ID}" | jq -r .job_status)
      if [ "$AUTOCLASS_STATUS" == "succeeded" ]; then
        echo "Bucket autoclass upgrade succeeded for job ${JOB_ID}"
        break
      elif [ "$AUTOCLASS_STATUS" == "failed" ]; then
        AUTOCLASS_FAILURE=$(curl_get "${TDR_URL}/api/repository/v1/jobs/${JOB_ID}/result" | jq -r .message)
        echo "Bucket upgrade failed for job ${JOB_ID}: ${AUTOCLASS_FAILURE}"
        break
      else
        sleep 1
      fi
    done
  done
}

AUTH_TOKEN=$(gcloud auth print-access-token)
AUTOCLASS_JOBS=()

parse_cli_args "$@"
autoclass_submit_requests
autoclass_check_responses
