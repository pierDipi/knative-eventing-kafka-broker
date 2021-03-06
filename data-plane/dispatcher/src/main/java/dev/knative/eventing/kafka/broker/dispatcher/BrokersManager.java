package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.ObjectsReconciler;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brokers manager manges Broker and Trigger objects by instantiating and starting verticles based
 * on brokers configurations.
 *
 * <p>Note: {@link BrokersManager} is not thread-safe and it's not supposed to be shared between
 * threads.
 *
 * @param <T> trigger type.
 */
public final class BrokersManager<T> implements ObjectsReconciler<T> {

  private static final Logger logger = LoggerFactory.getLogger(BrokersManager.class);

  // Broker -> Trigger<T> -> AbstractVerticle
  private final Map<Broker, Map<Trigger<T>, AbstractVerticle>> brokers;

  private final Vertx vertx;
  private final ConsumerVerticleFactory<T> consumerFactory;
  private final int triggersInitialCapacity;

  /**
   * All args constructor.
   *
   * @param vertx                   vertx instance.
   * @param consumerFactory         consumer factory.
   * @param brokersInitialCapacity  brokers container initial capacity.
   * @param triggersInitialCapacity triggers container initial capacity.
   */
  public BrokersManager(
      final Vertx vertx,
      final ConsumerVerticleFactory<T> consumerFactory,
      final int brokersInitialCapacity,
      final int triggersInitialCapacity) {

    Objects.requireNonNull(vertx, "provide vertx instance");
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (brokersInitialCapacity <= 0) {
      throw new IllegalArgumentException("brokersInitialCapacity cannot be negative or 0");
    }
    if (triggersInitialCapacity <= 0) {
      throw new IllegalArgumentException("triggersInitialCapacity cannot be negative or 0");
    }
    this.vertx = vertx;
    this.consumerFactory = consumerFactory;
    this.triggersInitialCapacity = triggersInitialCapacity;
    brokers = new HashMap<>(brokersInitialCapacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Future<Void> reconcile(final Map<Broker, Set<Trigger<T>>> newObjects) {
    final List<Future> futures = new ArrayList<>(newObjects.size() * 2);

    // diffing previous and new --> remove deleted objects
    for (final var brokersIt = brokers.entrySet().iterator(); brokersIt.hasNext(); ) {

      final var brokerTriggers = brokersIt.next();
      final var broker = brokerTriggers.getKey();

      // check if the old broker has been deleted or updated.
      if (!newObjects.containsKey(broker)) {

        // broker deleted or updated, so remove it
        brokersIt.remove();

        // undeploy all verticles associated with triggers of the deleted broker.
        for (final var e : brokerTriggers.getValue().entrySet()) {
          futures.add(undeploy(broker, e.getKey(), e.getValue()));
        }

        continue;
      }

      // broker is there, so check if some triggers have been deleted.
      final var triggersVerticles = brokerTriggers.getValue();
      for (final var triggersIt = triggersVerticles.entrySet().iterator(); triggersIt.hasNext(); ) {

        final var triggerVerticle = triggersIt.next();

        // check if the trigger has been deleted or updated.
        if (!newObjects.get(broker).contains(triggerVerticle.getKey())) {

          // trigger deleted or updated, so remove it
          triggersIt.remove();

          // undeploy verticle associated with the deleted trigger.
          futures.add(undeploy(
              broker,
              triggerVerticle.getKey(),
              triggerVerticle.getValue()
          ));
        }
      }
    }

    // add all new objects.
    for (final var entry : newObjects.entrySet()) {
      final var broker = entry.getKey();
      for (final var trigger : entry.getValue()) {

        futures.add(addBroker(broker, trigger));

      }
    }

    return CompositeFuture.all(futures).mapEmpty();
  }

  private Future<Void> addBroker(final Broker broker, final Trigger<T> trigger) {
    final Map<Trigger<T>, AbstractVerticle> triggers;

    if (brokers.containsKey(broker)) {
      triggers = brokers.get(broker);
    } else {
      triggers = new ConcurrentHashMap<>(triggersInitialCapacity);
      brokers.put(broker, triggers);
    }

    if (trigger == null || triggers.containsKey(trigger)) {
      // the trigger is already there and it hasn't been updated.
      return Future.succeededFuture();
    }

    final Promise<Void> promise = Promise.promise();

    consumerFactory.get(broker, trigger)
        .compose(verticle -> startVerticle(verticle, broker, triggers, trigger, promise))
        .onSuccess(ignored -> promise.tryComplete())
        .onFailure(cause -> {
          // probably configuration are wrong, so do not retry.
          logger.error(
              "potential control-plane bug: failed to get verticle: {} - cause: {}",
              trigger,
              cause
          );
          promise.tryFail(cause);
        });

    return promise.future();
  }

  private Future<Void> startVerticle(
      final AbstractVerticle verticle,
      final Broker broker,
      final Map<Trigger<T>, AbstractVerticle> triggers,
      final Trigger<T> trigger,
      Promise<Void> promise) {

    addTrigger(triggers, trigger, verticle)
        .onSuccess(msg -> {
          logger.info(
              "verticle for trigger {} associated with broker {} deployed - message: {}",
              trigger,
              broker,
              msg
          );
          promise.tryComplete();
        })
        .onFailure(cause -> {
          // this is a bad state we cannot start the verticle for consuming messages.
          logger.error(
              "failed to start verticle for trigger {} associated with broker {} - cause {}",
              trigger,
              broker,
              cause
          );
          promise.tryFail(cause);
        });

    return promise.future();
  }

  private Future<String> addTrigger(
      final Map<Trigger<T>, AbstractVerticle> triggers,
      final Trigger<T> trigger,
      final AbstractVerticle verticle) {

    triggers.put(trigger, verticle);
    final Promise<String> promise = Promise.promise();
    vertx.deployVerticle(verticle, promise);
    return promise.future();
  }

  private Future<Void> undeploy(Broker broker, Trigger<T> trigger, AbstractVerticle verticle) {
    final Promise<Void> promise = Promise.promise();

    vertx.undeploy(verticle.deploymentID(), result -> {
      if (result.succeeded()) {
        logger.info("removed trigger {} associated with broker {}", trigger, broker);
        promise.tryComplete();
        return;
      }

      logger.error(
          "failed to un-deploy verticle for trigger {} associated with broker {} - cause",
          trigger,
          broker,
          result.cause()
      );
      promise.tryFail(result.cause());
    });

    return promise.future();
  }
}
