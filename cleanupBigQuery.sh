#!/bin/bash

set -e

if [[ $# -lt 1 ]]; then
    echo 'Usage: ./cleanupBigQuery.sh [--force-yes] <project>'
    exit 1
fi

PROJ=${1}

if [[ $# -eq 1 ]]; then
    PROJ=${1}
elif [[ ${1} = "--force-yes" ]]; then
    SKIP_CONFIRM=true
    PROJ=${2}
else
    echo "Invalid flag ${1}"
    exit 1
fi

confirm () {
    if [[ ! -z "$SKIP_CONFIRM" ]]; then
        shift
        $@
    else 
        # call with a prompt string or use a default
        read -r -p "${1:-Are you sure?} [y/N] " response
        case $response in
            [yY])
                shift
                $@
                ;;
            *)
                return 1
                ;;
        esac
    fi
}

DATASETS_TO_DELETE=`bq --project_id=$PROJ ls | tail -n +3`
for file in $DATASETS_TO_DELETE
do
  echo $file
done

if confirm "Delete all these Big Query datasets?"; then
    for file in $DATASETS_TO_DELETE
    do
      echo "Removing $file"
      bq rm -rf --project_id=$PROJ $file
    done
fi
