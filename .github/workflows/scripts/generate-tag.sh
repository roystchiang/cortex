#!/bin/bash

set -x
REF="$(cut -d'/' -f 3 <<< $GH_REF)"
[[ $REF = "master" ]] && TAG="" || TAG="$REF"
WORKING_SUFFIX=$(if git status --porcelain | grep -qE '^(?:[^?][^ ]|[^ ][^?])\s'; then echo "-WIP"; else echo ""; fi)
BRANCH_PREFIX=$(git rev-parse --abbrev-ref HEAD)
GIT_COMMIT="${BRANCH_PREFIX//\//-}-$(git rev-parse --short HEAD)$WORKING_SUFFIX"
echo "DOCKER_IMAGE_TAG=${TAG:-$GIT_COMMIT}" >> $GITHUB_ENV