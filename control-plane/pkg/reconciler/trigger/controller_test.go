package trigger

import (
	"testing"

	"go.uber.org/zap/zaptest"
	"knative.dev/pkg/configmap"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger"

	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger/fake"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	controller := NewController(ctx, configmap.NewStaticWatcher())
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func TestEnqueueTriggers(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	lister := triggerinformer.Get(ctx).Lister()

	enqueueTriggers(zaptest.NewLogger(t), lister, func(obj interface{}) {

	})
}
