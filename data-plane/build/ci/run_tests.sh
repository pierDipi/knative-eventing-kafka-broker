#!/usr/bin/env bash

cd data-plane || exit 1
docker build --file ./build/test/Dockerfile --tag tests .
