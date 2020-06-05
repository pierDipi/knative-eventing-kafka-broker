package dev.knative.eventingkafkabroker.receiver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.TextNode;
import dev.knative.eventingkafkabroker.receiver.CloudEventRequestToRecordMapper;
import dev.knative.eventingkafkabroker.receiver.HttpVerticle;
import dev.knative.eventingkafkabroker.receiver.ProducerDriver;
import dev.knative.eventingkafkabroker.receiver.RequestHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Message;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxHttpClientRequestMessageVisitor;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleTest {

  static final int TIMEOUT = 10;
  static final int PORT = 8080;

  @Test
  public void requestHandler(final Vertx vertx, final VertxTestContext testContext)
      throws Throwable {

    final var httpClient = vertx.createHttpClient();
    final var testCases = requestStream();
    final var countDown = new CountDownLatch(testCases.size());
    final var flags = testContext.checkpoint(testCases.size() * 2);

    final var producerDriver = new ProducerDriver<String, CloudEvent>();
    final var producer = producerDriver.producer(false);

    final var handler = new RequestHandler<>(producer, new CloudEventRequestToRecordMapper());

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    final var verticle = new HttpVerticle(httpServerOptions, handler);

    vertx.deployVerticle(verticle, testContext.succeeding(
        h -> testCases.parallelStream().map(tc -> tc.requestResponse).forEach(requestResponse -> {

          final var request = httpClient
              .post(PORT, "localhost", requestResponse.path)
              .exceptionHandler(testContext::failNow)
              .setTimeout(TIMEOUT * 500)
              .handler(response -> testContext.verify(() -> {
                    assertEquals(requestResponse.responseStatusCode, response.statusCode());
                    flags.flag();
                    countDown.countDown();
                  }
              ));

          requestResponse.requestConsumer.accept(request);
        })));

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    testContext.verify(() -> assertEquals(
        testCases.stream().filter(tc -> tc.record != null).count(),
        producerDriver.size()
    ));
    testContext.verify(() -> testCases.stream().map(tc -> tc.record).parallel().forEach(record -> {
      if (record != null) {
        producerDriver.containsOfFail(record);
      }
      flags.flag();
    }));
  }

  public static Collection<TestCase> requestStream() {
    return Arrays.asList(
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name",
                requestConsumer(new CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new KafkaProducerRecordImpl<>(
                "broker-ns-broker-name",
                "",
                new CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()
            )
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name/hello",
                requestConsumer(new CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name/",
                requestConsumer(new io.cloudevents.core.v03.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new KafkaProducerRecordImpl<>(
                "broker-ns-broker-name",
                "",
                new io.cloudevents.core.v03.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()
            )
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name/",
                request -> {
                  request.end("this is not a cloud event");
                },
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name/hello",
                request -> {
                  request.end("this is not a cloud event");
                },
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name",
                request -> {
                  final var objectMapper = new ObjectMapper();
                  final var objectNode = objectMapper.createObjectNode();
                  objectNode.set("hello", new FloatNode(1.24f));
                  objectNode.set("data", objectNode);
                  request.headers().set("Content-Type", "application/json");
                  request.end(objectNode.asText());
                },
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name",
                request -> {
                  final var objectMapper = new ObjectMapper();
                  final var objectNode = objectMapper.createObjectNode();
                  objectNode.set("specversion", new TextNode("1.0"));
                  objectNode.set("type", new TextNode("my-type"));
                  objectNode.set("source", new TextNode("my-source"));
                  objectNode.set("data", objectNode);
                  request.headers().set("Content-Type", "application/json");
                  request.end(objectNode.asText());
                },
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name2",
                requestConsumer(new io.cloudevents.core.v1.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new KafkaProducerRecordImpl<>(
                "broker-ns-broker-name2",
                "",
                new io.cloudevents.core.v1.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()
            )
        )
    );
  }

  private static Consumer<HttpClientRequest> requestConsumer(final CloudEvent event) {
    return request -> Message.writeBinaryEvent(
        event,
        VertxHttpClientRequestMessageVisitor.create(request)
    );
  }

  public static final class TestCase {

    final RequestResponse requestResponse;
    final KafkaProducerRecord<String, CloudEvent> record;

    public TestCase(
        final RequestResponse requestResponse,
        final KafkaProducerRecord<String, CloudEvent> record) {

      this.requestResponse = requestResponse;
      this.record = record;
    }
  }

  public static final class RequestResponse {

    final String path;
    final Consumer<HttpClientRequest> requestConsumer;
    final int responseStatusCode;

    public RequestResponse(
        final String path,
        final Consumer<HttpClientRequest> requestConsumer,
        final int responseStatusCode) {

      this.path = path;
      this.requestConsumer = requestConsumer;
      this.responseStatusCode = responseStatusCode;
    }
  }
}
