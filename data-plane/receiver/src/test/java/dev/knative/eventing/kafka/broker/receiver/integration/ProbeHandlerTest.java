package dev.knative.eventing.kafka.broker.receiver.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.SimpleProbeHandlerDecorator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ProbeHandlerTest {

  private static final int TIMEOUT = 5000;
  private static final int PORT = 8989;

  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final int OK = HttpResponseStatus.OK.code();
  private static final int NEXT_HANDLER_STATUS_CODE = HttpResponseStatus.SERVICE_UNAVAILABLE.code();

  private static HttpClient httpClient;

  @BeforeAll
  public static void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        LIVENESS_PATH,
        READINESS_PATH,
        r -> r.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end()
    ));
    httpClient = vertx.createHttpClient();
    vertx.deployVerticle(verticle, context.succeeding(ar -> context.completeNow()));
  }

  @AfterAll
  public static void tearDown() {
    httpClient.close();
  }

  @Test
  public void testReadinessCheck(final VertxTestContext context) {
    doRequest(READINESS_PATH)
        .onSuccess(statusCode -> context.verify(() -> {
          assertThat(statusCode).isEqualTo(OK);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  @Test
  public void testLivenessCheck(final VertxTestContext context) {
    doRequest(LIVENESS_PATH)
        .onSuccess(statusCode -> context.verify(() -> {
          assertThat(statusCode).isEqualTo(OK);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  @Test
  public void shouldForwardToNextHandler(final VertxTestContext context) {
    doRequest("/does-not-exists-42")
        .onSuccess(statusCode -> context.verify(() -> {
          assertThat(statusCode).isEqualTo(NEXT_HANDLER_STATUS_CODE);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  private static Future<Integer> doRequest(final String path) {
    final Promise<Integer> promise = Promise.promise();

    httpClient.get(PORT, "localhost", path)
        .exceptionHandler(promise::tryFail)
        .setTimeout(TIMEOUT)
        .handler(response -> promise.tryComplete(response.statusCode()))
        .end();

    return promise.future();
  }
}
