#!/bin/sh
# Compatibility entrypoint; all inputs and the exclusive destination are explicit.
set -eu
exec python3 "$(dirname "$0")/build-toolchain.py" "$@"
