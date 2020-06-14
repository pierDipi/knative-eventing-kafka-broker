package dev.knative.eventing.kafka.broker.dispatcher.http;

import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxHttpClientRequestMessageWriter;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.net.URI;
import java.util.Objects;

public final class HttpConsumerRecordSender implements
    ConsumerRecordSender<String, CloudEvent, HttpClientResponse> {

  private final HttpClient client;
  private final String subscriberURI;
  private final CircuitBreaker circuitBreaker;

  /**
   * All args constructor.
   *
   * @param circuitBreaker circuit breaker to use.
   * @param client         http client.
   * @param subscriberURI  subscriber URI
   */
  public HttpConsumerRecordSender(
      final CircuitBreaker circuitBreaker,
      final HttpClient client,
      final String subscriberURI) {

    Objects.requireNonNull(circuitBreaker, "provide circuit breaker");
    Objects.requireNonNull(client, "provide client");
    Objects.requireNonNull(subscriberURI, "provide subscriber URI");
    if (subscriberURI.equals("") || !URI.create(subscriberURI).isAbsolute()) {
      throw new IllegalArgumentException("provide a valid subscriber URI");
    }

    this.circuitBreaker = circuitBreaker;
    this.client = client;
    this.subscriberURI = subscriberURI;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<HttpClientResponse> send(final KafkaConsumerRecord<String, CloudEvent> record) {
    final Promise<HttpClientResponse> promise = Promise.promise();
    // TODO un-comment when fixed
    // return circuitBreaker.execute(promise -> {
    final var request = client.postAbs(subscriberURI)
        .exceptionHandler(promise::tryFail)
        .handler(response -> {
          if (response.statusCode() >= 300) {
            // TODO determine which status codes are retryable
            //  (channels -> https://github.com/knative/eventing/issues/2411).
            promise.tryFail("response status code is not 2xx - got: " + response.statusCode());

            return;
          }

          promise.tryComplete(response);
        });

    VertxHttpClientRequestMessageWriter.create(request).writeBinary(record.value());
    // });

    return promise.future();
  }
}
