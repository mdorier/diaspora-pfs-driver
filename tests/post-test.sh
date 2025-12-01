#!/usr/bin/env bash

echo "================================================"
echo "After test"
echo "================================================"

RET=$1 # This is the return code of the test
echo "Test return $RET"

rm -rf ./diaspora_data || true

echo "================================================"
