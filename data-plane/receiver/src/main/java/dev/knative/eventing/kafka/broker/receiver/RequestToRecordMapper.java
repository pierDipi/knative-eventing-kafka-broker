package dev.knative.eventing.kafka.broker.receiver;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.function.Function;

@FunctionalInterface
public interface RequestToRecordMapper<K, V> extends
    Function<HttpServerRequest, Future<KafkaProducerRecord<K, V>>> {

  /**
   * Map the given HTTP request to a Kafka record.
   *
   * @param request http request.
   * @return kafka record (record can be null).
   */
  @Override
  Future<KafkaProducerRecord<K, V>> apply(HttpServerRequest request);
}
