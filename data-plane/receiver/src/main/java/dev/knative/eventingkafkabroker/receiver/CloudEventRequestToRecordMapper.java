package dev.knative.eventingkafkabroker.receiver;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Message;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.function.Consumer;

public class CloudEventRequestToRecordMapper implements RequestToRecordMapper<String, CloudEvent> {

  static final int PATH_TOKEN_NUMBER = 2;
  static final String PATH_DELIMITER = "/";
  static final String TOPIC_DELIMITER = "-";

  @Override
  public void accept(
      final HttpServerRequest request,
      final Consumer<Optional<KafkaProducerRecord<String, CloudEvent>>> recordConsumer) {

    VertxMessageFactory.fromHttpServerRequest(request)
        .map(Message::toEvent)
        .onComplete(result -> {

          if (result.failed()) {
            recordConsumer.accept(Optional.empty());
            return;
          }

          final var topic = topic(request.path());
          if (topic.isEmpty()) {
            recordConsumer.accept(Optional.empty());
            return;
          }

          // TODO(pierDipi) set the correct producer record key
          final var record = KafkaProducerRecord.create(topic.get(), "", result.result());
          recordConsumer.accept(Optional.of(record));
        });
  }

  static Optional<String> topic(final String path) {
    // The expected request path is of the form `/<broker-namespace>/<broker-name>`, that maps to
    // topic `<broker-namespace>-<broker-name>`, so, validate path and return topic name.
    // In case such topic doesn't exists the following apply:
    //  - The Broker is not Ready

    final var tokenizer = new StringTokenizer(path, PATH_DELIMITER, false);
    if (tokenizer.countTokens() != PATH_TOKEN_NUMBER) {
      return Optional.empty();
    }

    return Optional.of(String.join(TOPIC_DELIMITER, tokenizer.nextToken(), tokenizer.nextToken()));
  }
}
