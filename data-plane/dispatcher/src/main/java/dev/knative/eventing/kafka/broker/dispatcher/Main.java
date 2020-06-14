package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.ObjectsCreator;
import dev.knative.eventing.kafka.broker.dispatcher.file.FileWatcher;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final String BROKERS_TRIGGERS_PATH = "BROKERS_TRIGGERS_PATH";
  private static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  private static final String CONSUMER_CONFIG_FILE_PATH = "CONSUMER_CONFIG_FILE_PATH";
  private static final String BROKERS_INITIAL_CAPACITY = "BROKERS_INITIAL_CAPACITY";
  private static final String TRIGGERS_INITIAL_CAPACITY = "TRIGGERS_INITIAL_CAPACITY";
  private static final String RETRY_DELAY_AFTER_FAILED_VERTICLE_DEPLOY_MS
      = "RETRY_DELAY_AFTER_FAILED_VERTICLE_DEPLOY_MS";

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {

    final var envConfigs = new ConfigStoreOptions()
        .setType("env")
        .setOptional(false)
        .setConfig(new JsonObject().put("raw-data", true));

    final var configRetrieverOptions = new ConfigRetrieverOptions()
        .addStore(envConfigs);

    final var vertx = Vertx.vertx();
    final var configRetriever = ConfigRetriever.create(vertx, configRetrieverOptions);

    Future.future(configRetriever::getConfig)
        .onSuccess(json -> {
          final var watcherThread = new Thread(() -> start(vertx, json));
          Runtime.getRuntime().addShutdownHook(new Thread(watcherThread::interrupt));
          watcherThread.start();
        })
        .onFailure(cause -> {
          logger.error("failed to retrieve configurations", cause);
          shutdown(vertx);
        });

  }

  private static void start(final Vertx vertx, final JsonObject json) {

    final var producerConfigs = config(json.getString(PRODUCER_CONFIG_FILE_PATH));
    final var consumerConfigs = config(json.getString(CONSUMER_CONFIG_FILE_PATH));

    final ConsumerOffsetManagerFactory<String, CloudEvent> consumerOffsetManagerFactory
        = ConsumerOffsetManagerFactory.create();

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
        consumerOffsetManagerFactory,
        consumerConfigs,
        vertx.createHttpClient(),
        vertx,
        producerConfigs
    );

    final var brokersManager = new BrokersManager<>(
        vertx,
        consumerVerticleFactory,
        Integer.parseInt(json.getString(BROKERS_INITIAL_CAPACITY)),
        Integer.parseInt(json.getString(TRIGGERS_INITIAL_CAPACITY)),
        Long.parseLong(json.getString(RETRY_DELAY_AFTER_FAILED_VERTICLE_DEPLOY_MS))
    );

    final var objectCreator = new ObjectsCreator(brokersManager);

    try {
      final var fw = new FileWatcher(
          FileSystems.getDefault().newWatchService(),
          objectCreator,
          new File(json.getString(BROKERS_TRIGGERS_PATH))
      );
      fw.watch();
    } catch (IOException | InterruptedException e) {
      logger.error("watcher exception", e);
    } finally {
      vertx.close();
    }
  }

  private static void shutdown(Vertx vertx) {
    vertx.close(ignored -> System.exit(1));
  }

  private static Properties config(final String path) {
    if (path == null) {
      return new Properties();
    }

    final var consumerConfigs = new Properties();
    try (final var configReader = new FileReader(path)) {
      consumerConfigs.load(configReader);
    } catch (IOException e) {
      logger.error("failed to load configurations from file {} - cause {}", path, e);
    }

    return consumerConfigs;
  }
}
