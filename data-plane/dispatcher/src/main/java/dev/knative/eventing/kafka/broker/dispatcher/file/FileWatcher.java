package dev.knative.eventing.kafka.broker.dispatcher.file;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.proto.BrokersConfig.Brokers;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileWatcher is the class responsible for watching a given file and reports update.
 */
public class FileWatcher {

  private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

  private final Consumer<Brokers> brokersConsumer;

  private final WatchService watcher;
  private final Path toWatchParentPath;
  private final Path toWatchPath;
  private final File toWatch;

  private final AtomicBoolean watching = new AtomicBoolean();

  /**
   * All args constructor.
   *
   * @param watcher         watch service
   * @param brokersConsumer updates receiver.
   * @param file            file to watch
   * @throws IOException watch service cannot be registered.
   */
  public FileWatcher(
      final WatchService watcher,
      final Consumer<Brokers> brokersConsumer,
      final File file)
      throws IOException {

    Objects.requireNonNull(brokersConsumer, "provide consumer");
    Objects.requireNonNull(file, "provide file");

    this.brokersConsumer = brokersConsumer;
    toWatch = file.getAbsoluteFile();
    logger.info("start watching {}", toWatch);

    toWatchPath = file.toPath();
    toWatchParentPath = file.getParentFile().toPath();

    this.watcher = watcher;
    toWatchParentPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
  }

  /**
   * Start watching.
   *
   * @throws IOException          see {@link BufferedInputStream#readAllBytes()}.
   * @throws InterruptedException see {@link WatchService#take()}
   */
  public void watch() throws IOException, InterruptedException {
    if (watching.getAndSet(true)) {
      return;
    }

    try {
      while (true) {
        var shouldUpdate = false;

        // Note: take() blocks
        final var key = watcher.take();
        logger.info("broker updates");

        if (!key.isValid()) {
          logger.warn("invalid key");
          continue;
        }

        for (final var event : key.pollEvents()) {

          final WatchEvent<Path> ev = cast(event);
          final var child = toWatchParentPath.resolve(ev.context());
          final var kind = event.kind();
          if (kind != OVERFLOW && child.equals(toWatchPath)) {
            shouldUpdate = true;
            break;
          }

        }

        if (shouldUpdate) {
          update();
        }

        key.reset();
      }
    } finally {
      watching.set(false);
    }
  }

  private void update() throws IOException {
    try (
        final var fileReader = new FileReader(toWatch);
        final var bufferedReader = new BufferedReader(fileReader)) {
      parseFromJson(bufferedReader);
    }
  }

  private void parseFromJson(final Reader content) throws IOException {
    try {
      final var brokers = Brokers.newBuilder();
      JsonFormat.parser().merge(content, brokers);
      brokersConsumer.accept(brokers.build());
    } catch (final InvalidProtocolBufferException ex) {
      logger.warn("failed to parse from JSON", ex);
    }
  }

  @SuppressWarnings("unchecked")
  static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>) event;
  }
}
