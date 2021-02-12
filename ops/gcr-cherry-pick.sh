#!/bin/sh

# set as either `api` or `ui`
INPUT_WHICH_REPO=

# set as the version of the code that will be cherry picked, for example 1.3.0
INPUT_IMG_VERSION=

GCR_DEV_URL="gcr.io/broad-jade-dev/jade-data-repo"
GCR_PUB_URL="gcr.io/datarepo-public-gcr/jade-data-repo"

GOOGLE_APPLICATION_CREDENTIALS="/tmp/gcr-public-sa.json"

gcr_public_auth() {
    vault read -format=json secret/dsde/datarepo/dev/gcr-sa-b64 | jq -r .data.key \
        | base64 --decode | tee ${GOOGLE_APPLICATION_CREDENTIALS}
    gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
}

cherry_pick_msg() {
    printf 'Cherry picking v%s of %s\n  from `%s`\n    to `%s`\n' "$1" "$2" "$3" "$4"
}

add_ui_url() {
    GCR_DEV_URL="${GCR_DEV_URL}-ui"
    GCR_PUB_URL="${GCR_PUB_URL}-ui"
}

add_version_url() {
    GCR_DEV_URL="${GCR_DEV_URL}:${INPUT_IMG_VERSION}"
    GCR_PUB_URL="${GCR_PUB_URL}:${INPUT_IMG_VERSION}"
}

gcr_cherry_pick() {
    case ${INPUT_WHICH_REPO} in
        api)
            add_version_url ;;
        ui)
            add_ui_url ; add_version_url ;;
        *)
            echo '${INPUT_WHICH_REPO} must be one of `api` or `ui`'; exit 1 ;;
    esac

    cherry_pick_msg "${INPUT_IMG_VERSION}" "${INPUT_WHICH_REPO}" "${GCR_DEV_URL}" "${GCR_PUB_URL}"

    gcloud container images add-tag --quiet "${GCR_DEV_URL}" "${GCR_PUB_URL}"
}

gcr_public_auth
gcr_cherry_pick
