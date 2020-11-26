#!/bin/bash

set -x
REF="$(cut -d'/' -f 3 <<< $GH_REF)"
[[ $REF = "master" ]] && TAG="" || TAG="$REF"
BRANCH_PREFIX=$(git rev-parse --abbrev-ref HEAD)
SHA=$(git rev-parse --short HEAD)
GIT_COMMIT="${BRANCH_PREFIX//\//-}-$SHA"
[[ $TAG = "" ]] && TAG="$GIT_COMMIT" || TAG="$TAG-$SHA"
echo "DOCKER_IMAGE_TAG=$TAG" >> $GITHUB_ENV
