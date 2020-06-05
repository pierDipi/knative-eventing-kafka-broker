#!/usr/bin/env bash

export KO_DOCKER_REPO=knative-kafka-broker
kind create cluster
kubectl wait -n kube-system --for=condition=ready --timeout=60s pods --all
cd data-plane || exit 1
./build/kafka_setup.sh
sleep 60
kubectl wait -n kafka --for=condition=ready --timeout=120s pods --all
./build/build --skip-push --with-kind
kubectl wait -n knative-eventing --for=condition=ready --timeout=60s pods --all
