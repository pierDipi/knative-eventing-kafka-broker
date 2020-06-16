#!/usr/bin/env bash

cd control-plane || exit 1

# install kntest
go install knative.dev/test-infra/kntest/cmd/kntest

REPO_ROOT_DIR=$(pwd) ./test/presubmit-tests.sh "$@"
