#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../" || return

UR_VERSION=${UR_VERSION:-"latest"}
UR_REGISTRY=${UR_REGISTRY:-openi}
IMAGES_DIR="build/images"
BASE_IMAGE=${BASE_IMAGE:-debian:bullseye}

CGO_ENABLED=${CGO_ENABLED:-0}
GOPROXY=${GOPROXY:-`go env -w GOPROXY=https://goproxy.cn,direct`}
GOTAGS=${GOTAGS:-}
GOGCFLAGS=${GOGCFLAGS:-}

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}

# enable bash debug output
DEBUG=${DEBUG:-}

if [[ -n "$DEBUG" ]]; then
    set -x
fi

docker-build() {
    name=$1
    docker build \
      --platform ${GOOS}/${GOARCH} \
      --build-arg CGO_ENABLED="${CGO_ENABLED}" \
      --build-arg GOPROXY="${GOPROXY}" \
      --build-arg GOTAGS="${GOTAGS}" \
      --build-arg GOGCFLAGS="${GOGCFLAGS}" \
      --build-arg BASE_IMAGE="${BASE_IMAGE}" \
      -t "${UR_REGISTRY}/${name}:${UR_VERSION}" \
      -f "${IMAGES_DIR}/${name}/Dockerfile" .
}

git-submodule() {
  git submodule update --init --recursive
}

main() {
    case "${1-}" in
    urchin-peerdaemon)
        docker-build urchin-peerdaemon
        ;;
    dfdaemon)
        docker-build dfdaemon
        ;;
    scheduler)
        docker-build scheduler
        ;;
    manager)
        git-submodule
        docker-build manager
    esac
}

main "$@"
