package dev.knative.eventingkafkabroker.receiver;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface RequestToRecordMapper<K, V> extends
    BiConsumer<HttpServerRequest, Consumer<KafkaProducerRecord<K, V>>> {

  /**
   * Map the given HttpServerRequest to a Kafka record.
   *
   * @param request        http server request.
   * @param recordConsumer Kafka producer record consumer (record parameter can be null)
   */
  @Override
  void accept(
      final HttpServerRequest request,
      final Consumer<KafkaProducerRecord<K, V>> recordConsumer);

}
