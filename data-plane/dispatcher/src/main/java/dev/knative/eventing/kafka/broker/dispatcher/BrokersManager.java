package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.ObjectsReconciler;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.HashMap;
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
  private final long retryDelayAfterFailedToDeployVerticleMs;

  /**
   * All args constructor.
   *
   * @param vertx                                   vertx instance.
   * @param consumerFactory                         consumer factory.
   * @param brokersInitialCapacity                  brokers container initial capacity.
   * @param triggersInitialCapacity                 triggers container initial capacity.
   * @param retryDelayAfterFailedToDeployVerticleMs delay to wait before retrying deploying
   *                                                verticles.
   */
  public BrokersManager(
      final Vertx vertx,
      final ConsumerVerticleFactory<T> consumerFactory,
      final int brokersInitialCapacity,
      final int triggersInitialCapacity,
      final long retryDelayAfterFailedToDeployVerticleMs) {

    Objects.requireNonNull(vertx, "provide vertx instance");
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (brokersInitialCapacity <= 0) {
      throw new IllegalArgumentException("brokersInitialCapacity cannot be negative or 0");
    }
    if (triggersInitialCapacity <= 0) {
      throw new IllegalArgumentException("triggersInitialCapacity cannot be negative or 0");
    }
    if (retryDelayAfterFailedToDeployVerticleMs <= 0) {
      throw new IllegalArgumentException(
          "retryDelayAfterFailedToDeployVerticleMs cannot be negative or 0"
      );
    }

    this.vertx = vertx;
    this.consumerFactory = consumerFactory;
    this.triggersInitialCapacity = triggersInitialCapacity;
    this.retryDelayAfterFailedToDeployVerticleMs = retryDelayAfterFailedToDeployVerticleMs;
    brokers = new HashMap<>(brokersInitialCapacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reconcile(final Map<Broker, Set<Trigger<T>>> objects) {

    // diffing previous and new --> remove deleted objects
    for (final var brokersIt = brokers.entrySet().iterator(); brokersIt.hasNext(); ) {
      final var entry = brokersIt.next();
      final var broker = entry.getKey();
      if (!objects.containsKey(broker)) {
        // broker deleted
        brokersIt.remove();
        for (final var e : entry.getValue().entrySet()) {
          undeploy(broker, e.getKey(), e.getValue());
        }
        continue;
      }

      for (final var triggersIt = entry.getValue().entrySet().iterator(); triggersIt.hasNext(); ) {
        final var triggerVerticle = triggersIt.next();
        if (!objects.get(broker).contains(triggerVerticle.getKey())) {
          // trigger deleted
          triggersIt.remove();
          undeploy(broker, triggerVerticle.getKey(), triggerVerticle.getValue());
        }
      }
    }

    // add all new objects
    for (final var entry : objects.entrySet()) {
      final var broker = entry.getKey();
      for (final var trigger : entry.getValue()) {
        addBroker(broker, trigger);
      }
    }
  }

  private void addBroker(final Broker broker, final Trigger<T> trigger) {
    final Map<Trigger<T>, AbstractVerticle> triggers;

    if (brokers.containsKey(broker)) {
      triggers = brokers.get(broker);
    } else {
      triggers = new ConcurrentHashMap<>(triggersInitialCapacity);
      brokers.put(broker, triggers);
    }

    if (trigger == null || triggers.containsKey(trigger)) {
      // the trigger is already there and it hasn't been updated.
      return;
    }

    consumerFactory.get(broker, trigger)
        .onSuccess(verticle -> startVerticle(verticle, broker, triggers, trigger))
        .onFailure(cause -> {
          // probably configuration are wrong, so do not retry.
          logger.error(
              "potential control-plane bug: failed to get verticle: {} - cause: {}",
              trigger,
              cause
          );
        });
  }

  private void startVerticle(
      final AbstractVerticle verticle,
      final Broker broker,
      final Map<Trigger<T>, AbstractVerticle> triggers,
      final Trigger<T> trigger) {

    addTrigger(triggers, trigger, verticle)
        .onSuccess(msg -> logger.info(
            "verticle for trigger {} associated with broker {} deployed - message: {}",
            trigger,
            broker,
            msg
        ))
        .onFailure(cause -> {
          // this is a bad state we cannot start the verticle for consuming messages.
          logger.error(
              "failed to start verticle for trigger {} associated with broker {} - cause {}",
              trigger,
              broker,
              cause
          );

          // retry to start verticle after the given delay.
          vertx.setTimer(retryDelayAfterFailedToDeployVerticleMs, id -> {
            logger.warn(
                "retry to start verticle for trigger {} associated with broker {}",
                trigger,
                broker
            );

            startVerticle(verticle, broker, triggers, trigger);
          });
        });
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

  private void undeploy(Broker broker, Trigger<T> trigger, AbstractVerticle verticle) {
    vertx.undeploy(verticle.deploymentID(), result -> {
      if (result.succeeded()) {
        logger.info("removed trigger {} associated with broker {}", trigger, broker);
        return;
      }

      logger.error(
          "failed to un-deploy verticle for trigger {} associated with broker {} - cause",
          trigger,
          broker,
          result.cause()
      );

      vertx.setTimer(retryDelayAfterFailedToDeployVerticleMs, id -> {
        logger.info("retry to remove trigger {} associated with broker {}", trigger, broker);
        undeploy(broker, trigger, verticle);
      });
    });
  }
}
