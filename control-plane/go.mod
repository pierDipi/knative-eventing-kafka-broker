module knative.dev/eventing-kafka-broker/control-plane

go 1.13

require (
	go.uber.org/zap v1.14.1
	k8s.io/api v0.17.6
	k8s.io/apimachinery v0.17.6
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	knative.dev/eventing v0.15.1-0.20200616085824-132bb601ee40
	knative.dev/pkg v0.0.0-20200616011124-086ff4395641
	knative.dev/test-infra v0.0.0-20200615231324-3a016f44102c
)

replace k8s.io/client-go => k8s.io/client-go v0.17.6
