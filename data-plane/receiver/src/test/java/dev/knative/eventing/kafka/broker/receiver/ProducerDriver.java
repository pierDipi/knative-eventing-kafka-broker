package dev.knative.eventing.kafka.broker.receiver;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class ProducerDriver<K, V> {

  private Set<Record<K, V>> records;

  public ProducerDriver() {
    records = new ConcurrentHashSet<>();
  }

  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> producer(final boolean failToSend) {

    final KafkaProducer<K, V> producer = mock(KafkaProducer.class);

    when(producer.send(any(), any())).thenAnswer(invocationOnMock -> {
      final KafkaProducerRecord<K, V> record = invocationOnMock
          .getArgument(0, KafkaProducerRecord.class);
      if (!failToSend) {
        add(record);
      }

      final var handler = (Handler<AsyncResult<RecordMetadata>>) invocationOnMock
          .getArgument(1, Handler.class);
      final var result = mock(AsyncResult.class);
      when(result.failed()).thenReturn(failToSend);
      when(result.succeeded()).thenReturn(!failToSend);

      handler.handle(result);

      return producer;
    });

    return producer;
  }

  public void add(KafkaProducerRecord<K, V> record) {
    Objects.requireNonNull(record, "provide record");

    records.add(new Record<>(record));
  }

  public boolean contains(final KafkaProducerRecord<K, V> record) {
    Objects.requireNonNull(record, "provide record");

    return records.contains(new Record<>(record));
  }

  public void containsOfFail(final KafkaProducerRecord<K, V> record) {
    if (!contains(record)) {
      fail(record + " not found\nrecords registered" + Arrays.deepToString(records.toArray()));
    }
  }

  public int size() {
    return records.size();
  }

  /**
   * Record class is used for comparing record.
   *
   * @param <K> record key type.
   * @param <V> record value type.
   */
  private static final class Record<K, V> {

    final String topic;
    final K key;
    final V value;

    public Record(final String topic, final K key, final V value) {
      Objects.requireNonNull(topic);
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);

      this.topic = topic;
      this.key = key;
      this.value = value;
    }

    public Record(final KafkaProducerRecord<K, V> record) {
      this(record.topic(), record.key(), record.value());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Record<?, ?> record = (Record<?, ?>) o;
      return topic.equals(record.topic) &&
          key.equals(record.key) &&
          value.equals(record.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, key, value);
    }

    @Override
    public String toString() {
      return "Record{" +
          "topic='" + topic + '\'' +
          ", key=" + key +
          ", value=" + value +
          '}';
    }
  }

}
