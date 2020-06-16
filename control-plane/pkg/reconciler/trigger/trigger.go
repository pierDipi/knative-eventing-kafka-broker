package trigger

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/reconciler"
)

const (
	trigger           = "Trigger"
	triggerReconciled = trigger + "Reconciled"
)

type Reconciler struct {
	logger *zap.Logger
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *v1beta1.Trigger) reconciler.Event {
	r.logger.Debug("Reconciling Trigger", zap.Any("trigger", trigger))

	return reconciledNormal(trigger.Namespace, trigger.Name)
}

func reconciledNormal(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(
		corev1.EventTypeNormal,
		triggerReconciled,
		fmt.Sprintf(`%s reconciled: "%s/%s"`, trigger, namespace, name),
	)
}
