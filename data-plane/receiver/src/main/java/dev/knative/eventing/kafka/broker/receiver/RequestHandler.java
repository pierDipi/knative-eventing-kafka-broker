package dev.knative.eventing.kafka.broker.receiver;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.Objects;

/**
 * RequestHandler is responsible for mapping HTTP requests to Kafka records, sending records to
 * Kafka through the Kafka producer and terminating requests with the appropriate status code.
 *
 * @param <K> type of the records' key.
 * @param <V> type of the records' value.
 */
public class RequestHandler<K, V> implements Handler<HttpServerRequest> {

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE_STATUS_CODE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();

  private final KafkaProducer<K, V> producer;
  private final RequestToRecordMapper<K, V> requestToRecordMapper;

  /**
   * Create a new Request handler.
   *
   * @param producer              kafka producer
   * @param requestToRecordMapper request to record mapper
   */
  public RequestHandler(
      final KafkaProducer<K, V> producer,
      final RequestToRecordMapper<K, V> requestToRecordMapper) {

    Objects.requireNonNull(producer, "provide a producer");
    Objects.requireNonNull(requestToRecordMapper, "provide a mapper");

    this.producer = producer;
    this.requestToRecordMapper = requestToRecordMapper;
  }

  @Override
  public void handle(final HttpServerRequest request) {
    requestToRecordMapper.apply(request).onComplete(result -> {
      final var record = result.result();
      if (record == null) {
        request.response().setStatusCode(MAPPER_FAILED).end();
        return;
      }

      producer.send(record, metadataResult -> {

        if (metadataResult.failed()) {
          request.response().setStatusCode(FAILED_TO_PRODUCE_STATUS_CODE).end();
          return;
        }

        request.response().setStatusCode(RECORD_PRODUCED).end();
      });

    });
  }
}
