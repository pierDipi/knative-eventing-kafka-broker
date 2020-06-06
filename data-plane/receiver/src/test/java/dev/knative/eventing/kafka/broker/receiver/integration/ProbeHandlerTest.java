package dev.knative.eventing.kafka.broker.receiver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
  private HttpVerticle verticle;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        LIVENESS_PATH,
        READINESS_PATH,
        r -> r.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end()
    ));
    httpClient = vertx.createHttpClient();
    vertx.deployVerticle(verticle, context.completing());
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
  public void testReadinessCheck(final Vertx vertx, final VertxTestContext context) {
    doRequest(READINESS_PATH)
        .onSuccess(statusCode -> context.verify(() -> {
          assertEquals(OK, statusCode);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  @Test
  public void testLivenessCheck(final Vertx vertx, final VertxTestContext context) {
    doRequest(LIVENESS_PATH)
        .onSuccess(statusCode -> context.verify(() -> {
          assertEquals(OK, statusCode);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }

  @Test
  public void shouldForwardToNextHandler(final Vertx vertx, final VertxTestContext context) {
    doRequest("/does-not-exists-42")
        .onSuccess(statusCode -> context.verify(() -> {
          assertEquals(NEXT_HANDLER_STATUS_CODE, statusCode);
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
