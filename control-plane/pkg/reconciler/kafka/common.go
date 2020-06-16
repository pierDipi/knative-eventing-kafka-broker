package kafka

import (
	pkgreconciler "knative.dev/pkg/reconciler"

	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
)

func BrokerFilter() func(interface{}) bool {
	return pkgreconciler.AnnotationFilterFunc(
		brokerreconciler.ClassAnnotationKey,
		BrokerClass,
		false, // allowUnset
	)
}
