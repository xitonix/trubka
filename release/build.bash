set -euxo pipefail

if [[ $# -ne 1 && $# -ne 2 ]]; then
  echo "usage: release/build.bash OUT [VERSION]" 1>&2
  exit 64
fi

BUILD_TIME=$(date -u '+%a %d %b %Y %H:%M:%S GMT')
RUNTIME=$(go version | cut -d' ' -f 3)

cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")"
VERSION="${2:-}"
go build -o "$1" -ldflags="-s -w -X main.version=v${VERSION} -X main.runtimeVer=${RUNTIME} -X main.commit=${GITHUB_SHA} -X 'main.built=${BUILD_TIME}'" *.go