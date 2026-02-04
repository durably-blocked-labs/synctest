#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
GO_SRC="$ROOT_DIR/go/src"

echo "Building Go from source..."
echo "Source: $GO_SRC"

cd "$GO_SRC"

# Build Go (uses existing Go to bootstrap)
./make.bash

echo ""
echo "Done! Custom Go built at:"
echo "  $ROOT_DIR/go/bin/go"
echo ""
echo "Test it:"
echo "  $ROOT_DIR/go/bin/go version"
