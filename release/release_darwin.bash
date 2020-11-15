set -euxo pipefail

SRC="$(dirname "$(dirname "${BASH_SOURCE[0]}")")"
RELEASE_VERSION=$(echo ${GITHUB_REF} | cut -d'v' -f2)
RELEASE_OS="$(go env GOOS)"
RELEASE_ARCH="$(go env GOARCH)"
RELEASE_NAME="${BINARY}_${RELEASE_VERSION}_${RELEASE_OS}_${RELEASE_ARCH}"
BIN_DIR="$(pwd)/output/usr/bin"
RPM_ITERATION=1
mkdir -p $BIN_DIR
echo "Creating ${RELEASE_NAME}.tar.gz..." 1>&2
"$SRC/release/build.bash" "$BIN_DIR/$BINARY" "$RELEASE_VERSION"
tar -C "${BIN_DIR}" -cvzf "${RELEASE_NAME}.tar.gz" "${BINARY}"
echo "::set-output name=file::${RELEASE_NAME}.tar.gz"
echo "::set-output name=sha::$(shasum -a 256 ${RELEASE_NAME}.tar.gz | awk '{printf $1}')"
echo "::set-output name=version::v${RELEASE_VERSION}"