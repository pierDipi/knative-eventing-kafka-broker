package dev.knative.eventing.kafka.broker.receiver.integration;

import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.TOPIC_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.TextNode;
import dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.RequestHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Message;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxHttpClientRequestMessageVisitor;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleTest {

  private static final int TIMEOUT = 10;
  private static final int PORT = 8082;

  private static HttpClient httpClient;

  private MockProducer<String, CloudEvent> producer;
  private AbstractVerticle verticle;

  @BeforeAll
  public static void setUp(final Vertx vertx) {
    httpClient = vertx.createHttpClient();
  }

  @BeforeEach
  public void deployVerticle(Vertx vertx, VertxTestContext testContext) {
    final var mockProducer = new MockProducer<>(
        true,
        new StringSerializer(),
        new CloudEventSerializer()
    );
    this.producer = mockProducer;
    final var producer = KafkaProducer.create(vertx, mockProducer);
    final var handler = new RequestHandler<>(producer, new CloudEventRequestToRecordMapper());

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    verticle = new HttpVerticle(httpServerOptions, handler);
    vertx.deployVerticle(verticle, testContext.completing());
  }

  @AfterEach
  void tearDown(final Vertx vertx, final VertxTestContext context) {
    vertx.undeploy(verticle.deploymentID(), context.completing());
  }

  @AfterAll
  public static void tearDown() {
    httpClient.close();
  }

  @Test
  public void testValidNonValidEvents(final VertxTestContext context) throws Throwable {
    testAll(context, validNonValidEvents());
  }

  @ParameterizedTest
  @MethodSource({"validNonValidEvents"})
  public void test(final TestCase testCase, final VertxTestContext context)
      throws InterruptedException {
    testAll(context, Collections.singleton(testCase));
  }

  private void testAll(
      final VertxTestContext context,
      final Collection<TestCase> testCases) throws InterruptedException {

    final var checkpoints = context.checkpoint(testCases.size());
    final var countDown = new CountDownLatch(testCases.size());

    testCases.stream().map(tc -> tc.requestResponse).forEach(rr -> {
      doRequest(rr.requestFinalizer, rr.path)
          .onSuccess(response -> context.verify(() -> {

                assertThat(response.statusCode())
                    .as("verify path: " + rr.path)
                    .isEqualTo(rr.responseStatusCode);

                checkpoints.flag();
                countDown.countDown();
              }
          ))
          .onFailure(context::failNow);
    });

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    final var expectedProducedRecord = testCases.stream()
        .filter(tc -> tc.record != null)
        .map(tc -> tc.record)
        .collect(Collectors.toList());

    assertThat(producer.history())
        .containsExactlyInAnyOrderElementsOf(expectedProducedRecord);
  }

  private static Future<HttpClientResponse> doRequest(
      final Consumer<HttpClientRequest> requestConsumer,
      final String path) {

    final Promise<HttpClientResponse> promise = Promise.promise();

    requestConsumer.accept(
        httpClient.post(PORT, "localhost", path)
            .exceptionHandler(promise::tryFail)
            .setTimeout(TIMEOUT * 500)
            .handler(promise::tryComplete)
    );

    return promise.future();
  }

  private static Collection<TestCase> validNonValidEvents() {
    return Arrays.asList(
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name1",
                ceRequestFinalizer(new CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new ProducerRecord<>(
                TOPIC_PREFIX + "broker-ns-broker-name1",
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
                ceRequestFinalizer(new CloudEventBuilder()
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
                "/broker-ns/h/hello",
                ceRequestFinalizer(new CloudEventBuilder()
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
                "/broker-ns/broker-name1/",
                ceRequestFinalizer(new io.cloudevents.core.v03.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new ProducerRecord<>(
                TOPIC_PREFIX + "broker-ns-broker-name1",
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
                request -> request.end("this is not a cloud event"),
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name/hello",
                request -> request.end("this is not a cloud event"),
                RequestHandler.MAPPER_FAILED
            ),
            null
        ),
        new TestCase(
            new RequestResponse(
                "/broker-ns/broker-name3",
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
                "/broker-ns/broker-name4",
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
                "/broker-ns/broker-name5",
                ceRequestFinalizer(new io.cloudevents.core.v1.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()),
                RequestHandler.RECORD_PRODUCED
            ),
            new ProducerRecord<>(
                TOPIC_PREFIX + "broker-ns-broker-name5",
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

  private static Consumer<HttpClientRequest> ceRequestFinalizer(final CloudEvent event) {
    return request -> Message.writeBinaryEvent(
        event,
        VertxHttpClientRequestMessageVisitor.create(request)
    );
  }

  public static final class TestCase {

    final RequestResponse requestResponse;
    final ProducerRecord<String, CloudEvent> record;

    public TestCase(
        final RequestResponse requestResponse,
        final ProducerRecord<String, CloudEvent> record) {

      this.requestResponse = requestResponse;
      this.record = record;
    }
  }

  public static final class RequestResponse {

    final String path;
    final Consumer<HttpClientRequest> requestFinalizer;
    final int responseStatusCode;

    public RequestResponse(
        final String path,
        final Consumer<HttpClientRequest> requestFinalizer,
        final int responseStatusCode) {

      this.path = path;
      this.requestFinalizer = requestFinalizer;
      this.responseStatusCode = responseStatusCode;
    }
  }
}
