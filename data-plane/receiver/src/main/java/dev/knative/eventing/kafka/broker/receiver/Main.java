package dev.knative.eventing.kafka.broker.receiver;

import static io.vertx.kafka.client.producer.KafkaProducer.create;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Start receiver.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {
    final var env = new Env(System::getenv);

    logger.info("receiver environment configuration {}", env);

    final var producerConfigs = new Properties();
    try (final var configReader = new FileReader(env.getProducerConfigFilePath())) {
      producerConfigs.load(configReader);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    final var vertx = Vertx.vertx();
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    final var producer = create(vertx, new KafkaProducer<String, CloudEvent>(producerConfigs));

    final var handler = new RequestHandler<>(producer, new CloudEventRequestToRecordMapper());
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(env.getIngressPort());
    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        env.getLivenessProbePath(), env.getReadinessProbePath(), handler
    ));

    vertx.deployVerticle(verticle, deployResult -> {
      if (deployResult.failed()) {
        logger.error("receiver not started", deployResult.cause());
        return;
      }

      logger.info("receiver started");
    });
  }
}
