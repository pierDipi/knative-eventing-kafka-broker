#!/usr/bin/env bash

readonly uuid=$(uuidgen --time)

readonly receiver="${KNATIVE_KAFKA_BROKER_RECEIVER:-knative-kafka-broker-receiver}":"${uuid}"
KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE="${KO_DOCKER_REPO}"/"${receiver}"

readonly dispatcher="${KNATIVE_KAFKA_BROKER_DISPATCHER:-knative-kafka-broker-dispatcher}":"${uuid}"
KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE="${KO_DOCKER_REPO}"/"${dispatcher}"

source build/common.sh

SKIP_PUSH=false
if [[ $1 == "--skip-push" ]]; then
  SKIP_PUSH=true
fi
WITH_KIND=false
if [[ $2 == "--with-kind" ]]; then
  WITH_KIND=true
fi

function push() {
  if ! ${SKIP_PUSH}; then
    docker push "$1"
  fi
}

function with_kind() {
  if ${WITH_KIND}; then
    kind load docker-image "$1"
  fi
}

function receiver_build_push() {
  echo "Building receiver ..."

  docker build -f build/receiver/Dockerfile -t "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}" . &&
    push "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}" &&
    with_kind "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}"

  return $?
}

function dispatcher_build_push() {
  echo "Building dispatcher ..."

  docker build -f build/dispatcher/Dockerfile -t "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}" . &&
    push "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}" &&
    with_kind "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}"

  return $?
}

function build() {
  receiver_build_push || fail_test "failed to build receiver"
  dispatcher_build_push || fail_test "failed to build dispatcher"
}

build
k8s apply

#docker image rm "${receiver}"
#docker image rm "${KO_DOCKER_REPO}"/"${receiver}"
#docker image rm "${dispatcher}"
#docker image rm "${KO_DOCKER_REPO}"/"${dispatcher}"
