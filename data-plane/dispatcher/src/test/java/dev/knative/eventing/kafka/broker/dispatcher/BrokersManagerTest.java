package dev.knative.eventing.kafka.broker.dispatcher;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger3;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger4;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@ExtendWith(VertxExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class BrokersManagerTest {

  @Test
  @Timeout(value = 2)
  public void shouldAddBrokerAndDeployVerticles(final Vertx vertx)
      throws Exception {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggers = numTriggers(brokers);

    final var position = new AtomicInteger(0);
    final var verticles = new Verticles(numTriggers);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(verticles.get(position.getAndIncrement())),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokers);
    verticles.waitStarted();

    assertThat(vertx.deploymentIDs()).hasSize(numTriggers);
    assertThat(position.get()).isEqualTo(numTriggers);
  }

  @Test
  @Timeout(value = 2)
  public void shouldNotDeployWhenFailedToGetVerticle(final Vertx vertx) {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.failedFuture(new UnsupportedOperationException()),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokers);

    assertThat(vertx.deploymentIDs()).hasSize(0);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticleWhenTriggerDeleted(final Vertx vertx) throws Exception {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of()
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var verticle = new Verticle();

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(verticle),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokersOld);
    verticle.waitStarted();
    assertThat(vertx.deploymentIDs()).hasSize(numTriggersOld);

    brokersManager.reconcile(brokersNew);
    verticle.waitStopped();
    assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticlesWhenBrokerDeleted(final Vertx vertx) throws Exception {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of()
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var position = new AtomicInteger(0);
    final var verticles = new Verticles(numTriggersOld);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(verticles.get(position.getAndIncrement())),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokersOld);
    verticles.waitStarted();
    assertThat(vertx.deploymentIDs()).hasSize(numTriggersOld);

    brokersManager.reconcile(brokersNew);
    verticles.waitStopped();
    assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopAndStartVerticlesWhenTriggerDeletedAndReAdded(final Vertx vertx)
      throws Exception {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of(trigger3(), trigger1()),
        broker2(), Set.of(trigger1())
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var position = new AtomicInteger(0);
    final var verticles = new Verticles(numTriggersOld);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(verticles.get(position.getAndIncrement())),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokersOld);
    verticles.waitStarted();

    final var oldDeployments = vertx.deploymentIDs();
    assertThat(oldDeployments).hasSize(numTriggersOld);

    verticles.reset();
    position.set(0);
    brokersManager.reconcile(brokersNew);
    verticles.waitStarted(0);

    assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
    assertThat(vertx.deploymentIDs()).contains(oldDeployments.toArray(new String[0]));

    position.set(0);
    brokersManager.reconcile(brokersOld);
    verticles.waitStarted(0);
    assertThat(vertx.deploymentIDs()).hasSize(numTriggersOld);
  }

  @Test
  @Timeout(value = 2)
  public void shouldDoNothingWhenTheStateIsTheSame(final Vertx vertx) throws InterruptedException {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggers = numTriggers(brokers);

    final var brokers2 = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );

    final var position = new AtomicInteger(0);
    final var verticles = new Verticles(numTriggers);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(verticles.get(position.getAndIncrement())),
        100,
        100,
        1000
    );

    brokersManager.reconcile(brokers);
    verticles.waitStarted();

    final var deployments = vertx.deploymentIDs();
    assertThat(deployments).hasSize(numTriggers);

    brokersManager.reconcile(brokers2);
    assertThat(vertx.deploymentIDs()).containsExactlyInAnyOrder(deployments.toArray(new String[0]));
  }

  @Test
  public void shouldThrowIfBrokersInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new BrokersManager<>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(),
        -1,
        100,
        1000
    ));
  }

  @Test
  public void shouldThrowIfTriggersInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new BrokersManager<>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(),
        100,
        -1,
        1000
    ));
  }

  @Test
  public void shouldThrowIfRetryDelayAfterFailedToDeployVerticleMsIsLessOrEqualToZero(
      final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new BrokersManager<>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(),
        100,
        100,
        -1
    ));
  }

  private static int numTriggers(Map<Broker, Set<Trigger<CloudEvent>>> brokers) {
    return brokers.values().stream().mapToInt(Set::size).sum();
  }

  /**
   * Mock verticle to receive start and stop signals.
   *
   * <p>The implementation is not thread-safe.
   */
  public static final class Verticle extends AbstractVerticle {

    private CountDownLatch started = new CountDownLatch(1);
    private CountDownLatch stopped = new CountDownLatch(1);

    @Override
    public void start() {
      started.countDown();
    }

    @Override
    public void stop() {
      stopped.countDown();
    }

    public void waitStopped() throws InterruptedException {
      stopped.await();
      Thread.sleep(50); // reduce flakiness
    }

    public void waitStarted() throws InterruptedException {
      started.await();
      Thread.sleep(50); // reduce flakiness
    }

    public void reset() {
      started = new CountDownLatch(1);
      stopped = new CountDownLatch(1);
    }
  }

  public static class Verticles {

    private final Verticle[] verticles;

    public Verticles(int count) {
      verticles = new Verticle[count];
      for (int i = 0; i < count; i++) {
        verticles[i] = new Verticle();
      }
    }

    public void waitStarted() throws InterruptedException {
      for (final var verticle : verticles) {
        verticle.waitStarted();
      }
    }

    public void waitStarted(int... idx) throws InterruptedException {
      for (final int i : idx) {
        verticles[i].waitStarted();
      }
    }

    public void waitStopped() throws InterruptedException {
      for (final var verticle : verticles) {
        verticle.waitStarted();
      }
    }

    public void waitStopped(int... idx) throws InterruptedException {
      for (final int i : idx) {
        verticles[i].waitStopped();
      }
    }

    public void reset() {
      for (final var verticle : verticles) {
        verticle.reset();
      }
    }

    public Verticle get(int i) {
      return verticles[i];
    }
  }
}