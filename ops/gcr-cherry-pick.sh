#!/bin/sh

INPUT_GET_REPO=api
INPUT_VERSION=1.3.0

GCR_DEV_URL="gcr.io/broad-jade-dev/jade-data-repo"
GCR_PUB_URL="gcr.io/datarepo-public-gcr/jade-data-repo"

cherry_pick_msg() {
    printf 'Cherry picking v%s of %s\n  from `%s`\n    to `%s`\n' "$1" "$2" "$3" "$4"
}

add_ui_url() {
    GCR_DEV_URL="${GCR_DEV_URL}-ui"
    GCR_PUB_URL="${GCR_PUB_URL}-ui"
}

add_version_url() {
    GCR_DEV_URL="${GCR_DEV_URL}:${INPUT_VERSION}"
    GCR_PUB_URL="${GCR_PUB_URL}:${INPUT_VERSION}"
}

gcr_cherry_pick() {
    case ${INPUT_GET_REPO} in
        api)
            true ;;
        ui)
            add_ui_url ;;
        *)
            echo 'must be one of `api` or `ui`'; exit 1 ;;
    esac

    add_version_url
    cherry_pick_msg "${INPUT_VERSION}" "${INPUT_GET_REPO}" "${GCR_DEV_URL}" "${GCR_PUB_URL}"

    gcloud container images add-tag --quiet "${GCR_DEV_URL}" "${GCR_PUB_URL}"
}

gcr_cherry_pick
