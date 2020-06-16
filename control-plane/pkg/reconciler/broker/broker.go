package broker

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/reconciler"
)

const (
	broker           = "KafkaBroker"
	brokerReconciled = broker + "Reconciled"
)

type Reconciler struct {
	recorder record.EventRecorder
	logger   *zap.Logger
}

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *v1beta1.Broker) reconciler.Event {

	r.logger.Debug("reconciling Broker", zap.Any("broker", broker))

	return reconciledNormal(broker.Namespace, broker.Name)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *v1beta1.Broker) reconciler.Event {
	return reconciledNormal(broker.Namespace, broker.Name)
}

func reconciledNormal(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(
		corev1.EventTypeNormal,
		brokerReconciled,
		fmt.Sprintf(`%s reconciled: "%s/%s"`, broker, namespace, name),
	)
}
