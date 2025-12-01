#!/usr/bin/env bash

HERE=$(dirname $0)

echo "================================================"
echo "Before Test"
echo "================================================"

mkdir -p ./diaspora_data
rm -rf ./diaspora_data/* || true

echo "==> File system is ready"
echo "================================================"
