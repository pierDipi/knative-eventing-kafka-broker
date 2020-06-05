package dev.knative.eventing.kafka.broker.receiver;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import java.util.Objects;

public class HttpVerticle extends AbstractVerticle {

  private final HttpServerOptions httpServerOptions;
  private Handler<HttpServerRequest> requestHandler;
  private HttpServer server;

  /**
   * Create a new HttpVerticle.
   *
   * @param httpServerOptions server options.
   * @param requestHandler    request handler.
   */
  public HttpVerticle(
      final HttpServerOptions httpServerOptions,
      final Handler<HttpServerRequest> requestHandler) {

    Objects.requireNonNull(httpServerOptions, "provide http server options");
    Objects.requireNonNull(requestHandler, "provide request handler");

    this.httpServerOptions = httpServerOptions;
    this.requestHandler = requestHandler;
  }

  @Override
  public void start() {
    server = vertx.createHttpServer(httpServerOptions)
        .requestHandler(requestHandler)
        .listen();
  }

  @Override
  public void stop() {
    server.close();
  }
}
