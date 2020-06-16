package main

import (
	"knative.dev/pkg/injection/sharedmain"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	component = "kafka-broker-controller"
)

func main() {
	sharedmain.Main(
		component,

		broker.NewController,
	)
}
