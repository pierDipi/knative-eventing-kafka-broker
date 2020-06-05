package dev.knative.eventingkafkabroker.receiver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class RequestHandlerTest {

  @Test
  public void shouldSendRecordAndTerminateRequestWithRecordProduced() {
    shouldSendRecord(false, RequestHandler.RECORD_PRODUCED);
  }

  @Test
  public void shouldSendRecordAndTerminateRequestWithFailedToProduce() {
    shouldSendRecord(true, RequestHandler.FAILED_TO_PRODUCE_STATUS_CODE);
  }

  @SuppressWarnings({"unchecked"})
  private static void shouldSendRecord(boolean failedToSend, int statusCode) {
    final var record = new KafkaProducerRecordImpl<Object, Object>(
        "topic", "key", "value", 10
    );

    final RequestToRecordMapper<Object, Object> mapper
        = (request, optionalConsumer) -> optionalConsumer.accept(record);

    final var producer = new ProducerDriver<>().producer(failedToSend);

    final var request = mock(HttpServerRequest.class);
    final var response = mockResponse(request, statusCode);

    final var handler = new RequestHandler<>(producer, mapper);
    handler.handle(request);

    verify(producer, times(1)).send(refEq(record), any());
    verifySetStatusCodeAndTerminateResponse(statusCode, response);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void shouldReturnBadRequestIfNoRecordCanBeCreated() {
    final var producer = mock(KafkaProducer.class);

    final RequestToRecordMapper<Object, Object> mapper
        = (request, optionalConsumer) -> optionalConsumer.accept(null);

    final var request = mock(HttpServerRequest.class);
    final var response = mockResponse(request, RequestHandler.MAPPER_FAILED);

    final var handler = new RequestHandler<Object, Object>(producer, mapper);
    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(RequestHandler.MAPPER_FAILED, response);
  }

  private static void verifySetStatusCodeAndTerminateResponse(
      final int statusCode,
      final HttpServerResponse response) {
    verify(response, times(1)).setStatusCode(statusCode);
    verify(response, times(1)).end();
  }

  private static HttpServerResponse mockResponse(
      final HttpServerRequest request,
      final int statusCode) {

    final var response = mock(HttpServerResponse.class);
    when(response.setStatusCode(statusCode)).thenReturn(response);

    when(request.response()).thenReturn(response);
    return response;
  }
}