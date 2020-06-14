package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public interface ConsumerOffsetManagerFactory<K, V> {

  static <K, V> ConsumerOffsetManagerFactory<K, V> create() {
    return new ConsumerOffsetManagerFactory<>() {
    };
  }

  default ConsumerOffsetManager<K, V> get(
      final KafkaConsumer<K, V> consumer,
      final Broker broker,
      final Trigger<V> trigger) {

    return new UnorderedConsumerOffsetManager<>(consumer);
  }
}
