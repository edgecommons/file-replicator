#!/usr/bin/env bash
#
# Custom GDK build for the file-replicator Rust Greengrass component.
#
# `gdk component build` invokes this (see gdk-config.json -> custom_build_command). The GDK contract
# for a custom build system is that this script must place:
#   - the recipe   in  greengrass-build/recipes/
#   - the artifact in  greengrass-build/artifacts/<ComponentName>/<ComponentVersion>/
# (GDK creates those folders before calling us.)
#
# The on-device artifact is built with the `greengrass` feature (Greengrass IPC), which is Linux-only
# (the SDK is a C-FFI crate needing libclang). Build on a Linux host (or WSL), or set EDGECOMMONS_TARGET
# to a Linux triple you have a toolchain for, e.g.:
#   EDGECOMMONS_TARGET=x86_64-unknown-linux-gnu ./build.sh
set -euo pipefail

COMPONENT_NAME="com.mbreissi.edgecommons.FileReplicator"
COMPONENT_VERSION="$(python3 -c 'import json; c = json.load(open("gdk-config.json"))["component"]; print(next(iter(c.values()))["version"])')"
BIN_NAME="file-replicator"

# Greengrass-mode features for the device build. Add edgecommons features as needed (e.g.
# "greengrass,cloudwatch"); destination features (dest-s3, ...) are added here as phases land.
FEATURES="${EDGECOMMONS_FEATURES:-greengrass}"
TARGET="${EDGECOMMONS_TARGET:-}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

echo "Building ${BIN_NAME} (release, features=${FEATURES})${TARGET:+ for ${TARGET}}..."
if [[ -n "${TARGET}" ]]; then
  cargo build --release --no-default-features --features "${FEATURES}" --target "${TARGET}"
  BIN_DIR="${TARGET_DIR}/${TARGET}/release"
else
  cargo build --release --no-default-features --features "${FEATURES}"
  BIN_DIR="${TARGET_DIR}/release"
fi

BIN_PATH="${BIN_DIR}/${BIN_NAME}"
[[ -f "${BIN_PATH}" ]] || BIN_PATH="${BIN_DIR}/${BIN_NAME}.exe"
if [[ ! -f "${BIN_PATH}" ]]; then
  echo "error: built binary not found in ${BIN_DIR}" >&2
  exit 1
fi

ARTIFACT_DIR="greengrass-build/artifacts/${COMPONENT_NAME}/${COMPONENT_VERSION}"
RECIPE_DIR="greengrass-build/recipes"
mkdir -p "${ARTIFACT_DIR}" "${RECIPE_DIR}"

cp "${BIN_PATH}" "${ARTIFACT_DIR}/${BIN_NAME}"
chmod +x "${ARTIFACT_DIR}/${BIN_NAME}" || true
cp recipe.yaml "${RECIPE_DIR}/recipe.yaml"

echo "Staged artifact -> ${ARTIFACT_DIR}/${BIN_NAME}"
echo "Staged recipe   -> ${RECIPE_DIR}/recipe.yaml"
