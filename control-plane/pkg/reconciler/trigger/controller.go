package trigger

import (
	"context"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/trigger"
	eventingv1beta1 "knative.dev/eventing/pkg/client/listers/eventing/v1beta1"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()

	reconciler := &Reconciler{
		logger: logging.FromContext(ctx).Desugar(),
	}

	impl := triggerreconciler.NewImpl(ctx, reconciler)

	// Filter Brokers and enqueue associated Triggers
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerFilter(),
		Handler:    enqueueTriggers(reconciler.logger, triggerLister, impl.Enqueue),
	})

	return impl
}

func enqueueTriggers(
	logger *zap.Logger,
	lister eventingv1beta1.TriggerLister,
	enqueue func(obj interface{})) cache.ResourceEventHandler {

	return controller.HandleAll(func(obj interface{}) {

		if broker, ok := obj.(*v1beta1.Broker); ok {

			selector := labels.SelectorFromSet(map[string]string{eventing.BrokerLabelKey: broker.Name})
			triggers, err := lister.Triggers(broker.Namespace).List(selector)
			if err != nil {
				logger.Warn("Failed to list triggers", zap.Any("broker", broker), zap.Error(err))
				return
			}

			for _, trigger := range triggers {
				enqueue(trigger)
			}
		}
	})
}
