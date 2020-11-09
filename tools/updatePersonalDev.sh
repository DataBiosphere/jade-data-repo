#!/bin/bash

# Updates personal dev environment
# Requires yq and helmfile to be installed
# Steps:
# 1. Grabs latest running vesion from dev, sets it as $LATEST_VERSION
# 2. Changes your api yaml to point to the latest version
# 3. Question 1: Do you want to apply these changes? [y/n]
#     With "y" response, ask a second Question:
#        Do you want to delete the api pod first? (only do this if you expect to need to run migrations)
#     then, proceeds to run helmfile apply to the change
# 4. Question 2: Do you want to commit these changes to the datarepo-helm-definitions repo? [y/n]
#     With "y" response, it will commit these changes to master

set -e

CWD=${PWD}

if [[ $# -lt 1 ]]; then
    echo 'Usage: ./updatePersonalDev.sh <namespace>'
    exit 1
fi

NAMESPACE=${1}

LATEST_VERSION=$(curl -s -X GET "https://jade.datarepo-dev.broadinstitute.org/configuration" -H "accept: application/json" | jq -r '.semVer|rtrimstr("-SNAPSHOT")')
echo "[INFO] Latest version on dev: $LATEST_VERSION"
CURR=$(yq r ../../datarepo-helm-definitions/dev/$NAMESPACE/datarepo-api.yaml image.tag)
echo "[INFO] Current version on $NAMESPACE dev env: $CURR"
if [[ "$LATEST_VERSION" == "$CURR" ]]; then
    echo "[INFO] ALREADY UP TO DATE."
else
    echo "[INFO] Changing directory to /datarepo-helm-definitions/dev/$NAMESPACE"
    cd ../../datarepo-helm-definitions/dev/$NAMESPACE
    echo "[INFO] Updating version in datarepo-api.yaml to $LATEST_VERSION"
    yq w -i datarepo-api.yaml image.tag $LATEST_VERSION
    echo "[INFO] Running helmfile diff (no changes made until helmfile apply is run)"
    helmfile diff
    echo "Do you want to apply these changes? [y/n]"
    read ans
    if [[ "$ans" == "y" ]]; then
        echo "[INFO] Do you want to delete the api pod first? (only do this if you expect to need to run migrations) [y/n]"
        read ans2
        if [[ "$ans2" == "y" ]]; then
            echo "[INFO] Deleteing api pod"
            helm delete -n $NAMESPACE $NAMESPACE-jade-datarepo-api
            sleep 5
        fi
        echo "[INFO] Applying changes using helmfile apply"
        helmfile apply
        echo "[INFO] $NAMESPACE dev env should be up to date to version $LATEST_VERSION. Please change gcloud console to confirm."
    else
        echo "[INFO] Changes not applied."
    fi
    echo "Do you want to commit these changes to the datarepo-helm-definitions repo? [y/n]"
    read ans3
    if [[ "$ans3" == "y" ]]; then
        echo "[INFO] Commiting changes to datarepo-helm-definitions"
        git pull
        git status
        git commit -a -m "Updating $NAMESPACE dev env to version $LATEST_VERSION"
        git push
        echo "[INFO] Latest changes should be pushed to datarepo-helm-definitions"
    else
        echo "[INFO] Changes not pushed to remote repo"
    fi  
fi

cd ${CWD}