#!/bin/bash

# Azure UUID
uuid="c672c3c9-ab54-4e19-827c-f2af329da814"

echo "This script profiles dataset-builder endpoints on Azure:"
echo " - getConcepts"
echo " - searchConcepts"
echo " - getConceptHierarchy"
echo " - getSnapshotBuilderCount"

echo "Testing with Azure UUID: $uuid"

# Authenticate with gcloud and obtain an access token
#gcloud auth login
#TOKEN=$(gcloud auth print-access-token)

# DataRepo URL
DATAREPO_URL="https://jade.datarepo-dev.broadinstitute.org"

endpoints=("getSnapshotBuilderCount" "getConceptHierarchy" "searchConcepts" "getConcepts")


for endpoint in "${endpoints[@]}"
do
  case $endpoint in
    "getSnapshotBuilderCount")
      echo "Processing getSnapshotBuilderCount endpoint"
      ;;
    "getConceptHierarchy")
      echo "Processing getConceptHierarchy endpoint"
      ;;
    "searchConcepts")
      echo "Processing searchConcepts endpoint"
      ;;
    "getConcepts")
      echo "Processing getConcepts endpoint"
      ;;
    *)
      echo "Unknown endpoint: $endpoint"
      ;;
  esac
done

#url="${DATAREPO_URL}/api/repository/v1/datasets/${uuid}/snapshotBuilder/concepts/19/search?searchText="
#
### Perform the curl request with time metrics
##response=$(curl -w "HTTP_CODE: %{http_code}\nTOTAL_TIME: %{time_total}\n" \
##  -X GET "$url" \
##  -H "Authorization: Bearer ${TOKEN}" \
##  -o /dev/null)
#
#response=$(curl -X GET "${url}" -H "Authorization: Bearer ${TOKEN}")
#echo "$response"

