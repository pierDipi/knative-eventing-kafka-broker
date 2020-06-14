package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.proto.BrokersConfig.Broker;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class BrokerWrapperTest {

  @Test
  public void idCallShouldBeDelegatedToWrappedBroker() {
    final var id = "123-42";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setId(id).build()
    );

    assertThat(broker.id()).isEqualTo(id);
  }

  @Test
  public void deadLetterSinkCallShouldBeDelegatedToWrappedBroker() {
    final var deadLetterSink = "http://localhost:9090/api";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setDeadLetterSink(deadLetterSink).build()
    );

    assertThat(broker.deadLetterSink()).isEqualTo(deadLetterSink);
  }

  @Test
  public void topicCallShouldBeDelegatedToWrappedBroker() {
    final var topic = "knative-topic";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setTopic(topic).build()
    );

    assertThat(broker.topic()).isEqualTo(topic);
  }

  /**
   * @return a stream of pairs that are semantically different.
   * @see dev.knative.eventing.kafka.broker.core.BrokerTest#testTriggerDifference(Entry)
   */
  public static Stream<Entry<dev.knative.eventing.kafka.broker.core.Broker, dev.knative.eventing.kafka.broker.core.Broker>> differentTriggersProvider() {
    return Stream.of(
        new SimpleImmutableEntry<>(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setId("1234-id")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        new SimpleImmutableEntry<>(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setTopic("kantive-topic")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        new SimpleImmutableEntry<>(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setDeadLetterSink("http:/localhost:9090")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        )
    );
  }

  /**
   * @return a stream of pairs that are semantically equivalent.
   * @see dev.knative.eventing.kafka.broker.core.BrokerTest#testTriggerDifference(Entry)
   */
  public static Stream<Entry<dev.knative.eventing.kafka.broker.core.Broker, dev.knative.eventing.kafka.broker.core.Broker>> equalTriggersProvider() {
    return Stream.of(
        new SimpleImmutableEntry<>(
            new BrokerWrapper(
                Broker.newBuilder()
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        new SimpleImmutableEntry<>(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setTopic("knative-topic")
                    .setId("1234-42")
                    .setDeadLetterSink("http://localhost:9090")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder()
                    .setId("1234-42")
                    .setTopic("knative-topic")
                    .setDeadLetterSink("http://localhost:9090")
                    .build()
            )
        )
    );
  }
}