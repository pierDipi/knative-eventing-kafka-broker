package broker

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)

	reconciler := &Reconciler{
		logger: logging.FromContext(ctx).Desugar(),
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.BrokerClass)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	triggerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if trigger, ok := obj.(*v1beta1.Trigger); ok {
				impl.EnqueueKey(types.NamespacedName{
					Namespace: trigger.Namespace,
					Name:      trigger.Spec.Broker,
				})
			}
		},
	))

	return impl
}
