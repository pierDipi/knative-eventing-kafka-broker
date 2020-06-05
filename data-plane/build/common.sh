#!/usr/bin/env bash

readonly CONFIG_DIR=config
readonly CONFIG_TEMPLATE_DIR="${CONFIG_DIR}"/template # no trailing slash

function fail_test() {
  echo "$1"
  exit 1
}

function k8s() {
  echo "dispatcher image ---> ${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}"
  echo "receiver image   ---> ${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}"

  export KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE="${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}"
  export KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE="${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}"

  kubectl "$@" -f ${CONFIG_DIR}
  for file in "${CONFIG_TEMPLATE_DIR}"/*; do
    if ! envsubst <"$file" | kubectl "$@" -f -; then
      #    if ! envsubst <"$file" | cat -; then
      fail_test "failed to $1 ${file}"
    fi
  done
}
