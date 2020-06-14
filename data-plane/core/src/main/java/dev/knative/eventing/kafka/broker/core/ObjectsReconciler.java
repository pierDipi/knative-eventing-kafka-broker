package dev.knative.eventing.kafka.broker.core;

import java.util.Map;
import java.util.Set;

@FunctionalInterface
public interface ObjectsReconciler<T> {

  void reconcile(Map<Broker, Set<Trigger<T>>> objects) throws Exception;
}
