#!/bin/bash
# Run all example and test modules with the custom Go toolchain
set -e

GO="$(pwd)/go/bin/go"
export GODEBUG=asyncpreemptoff=1

echo "=== explorer ==="
cd /Users/shubhaankar/github.com/research/synctest/explorer
$GO test -v -count=1 ./... 2>&1 | head -60
echo ""

echo "=== experiments ==="
cd /Users/shubhaankar/github.com/research/synctest/experiments
$GO test -v -count=1 ./... 2>&1 | head -60
echo ""

echo "=== examples (distributed_test, balance_test) ==="
cd /Users/shubhaankar/github.com/research/synctest/examples
$GO test -v -count=1 . 2>&1 | head -60
echo ""

echo "=== examples/jobrace ==="
cd /Users/shubhaankar/github.com/research/synctest/examples
$GO test -v -count=1 ./jobrace/ 2>&1 | head -60
echo ""

echo "=== examples/local_replayer ==="
cd /Users/shubhaankar/github.com/research/synctest/examples/local_replayer
$GO test -v -count=1 ./... 2>&1 | head -60
echo ""

echo "=== examples/replayer_scheduler ==="
cd /Users/shubhaankar/github.com/research/synctest/examples/replayer_scheduler
$GO test -v -count=1 ./... 2>&1 | head -60
echo ""

echo "=== examples/distributed_replayer ==="
cd /Users/shubhaankar/github.com/research/synctest/examples/distributed_replayer
$GO test -v -count=1 ./... 2>&1 | head -60
echo ""

echo "=== ALL DONE ==="
