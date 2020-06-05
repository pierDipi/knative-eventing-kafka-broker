package dev.knative.eventingkafkabroker.receiver;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Message;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.lang.Nullable;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.StringTokenizer;
import java.util.function.Consumer;

public class CloudEventRequestToRecordMapper implements RequestToRecordMapper<String, CloudEvent> {

  static final int PATH_TOKEN_NUMBER = 2;
  static final String PATH_DELIMITER = "/";
  static final String TOPIC_DELIMITER = "-";

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(
      final HttpServerRequest request,
      final Consumer<KafkaProducerRecord<String, CloudEvent>> recordConsumer) {

    VertxMessageFactory.fromHttpServerRequest(request)
        .map(Message::toEvent)
        .onComplete(result -> {

          if (result.failed()) {
            recordConsumer.accept(null);
            return;
          }

          final var topic = topic(request.path());
          if (topic == null) {
            recordConsumer.accept(null);
            return;
          }

          // TODO(pierDipi) set the correct producer record key
          final var record = KafkaProducerRecord.create(topic, "", result.result());
          recordConsumer.accept(record);
        });
  }

  @Nullable
  static String topic(final String path) {
    // The expected request path is of the form `/<broker-namespace>/<broker-name>`, that maps to
    // topic `<broker-namespace>-<broker-name>`, so, validate path and return topic name.
    // In case such topic doesn't exists the following apply:
    //  - The Broker is not Ready

    final var tokenizer = new StringTokenizer(path, PATH_DELIMITER, false);
    if (tokenizer.countTokens() != PATH_TOKEN_NUMBER) {
      return null;
    }

    return String.join(TOPIC_DELIMITER, tokenizer.nextToken(), tokenizer.nextToken());
  }
}
