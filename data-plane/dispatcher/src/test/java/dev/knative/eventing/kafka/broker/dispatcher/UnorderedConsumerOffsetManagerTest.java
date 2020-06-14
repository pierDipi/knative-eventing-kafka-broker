package dev.knative.eventing.kafka.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@ExtendWith(VertxExtension.class)
@Execution(value = ExecutionMode.CONCURRENT)
public class UnorderedConsumerOffsetManagerTest {

  private static final int TIMEOUT_MS = 200;

  @Test
  @SuppressWarnings("unchecked")
  public void recordReceived() {
    final KafkaConsumer<Object, Object> consumer = mock(KafkaConsumer.class);
    new UnorderedConsumerOffsetManager<>(consumer).recordReceived(null);

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
  }

  @Test
  public void shouldCommitSuccessfullyOnSuccessfullySentToSubscriber(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.successfullySentToSubscriber(kafkaConsumerRecord));
  }

  @Test
  public void shouldCommitSuccessfullyOnSuccessfullySentToDLQ(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.successfullySentToDLQ(kafkaConsumerRecord));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void failedToSendToDLQ() {
    final KafkaConsumer<Object, Object> consumer = mock(KafkaConsumer.class);
    new UnorderedConsumerOffsetManager<>(consumer).failedToSendToDLQ(null, null);

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
  }

  @Test
  public void ShouldCommitSuccessfullyOnRecordDiscarded(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.recordDiscarded(kafkaConsumerRecord));
  }

  private static <K, V> void shouldCommit(
      final Vertx vertx,
      final BiConsumer<KafkaConsumerRecord<K, V>, UnorderedConsumerOffsetManager<K, V>> rConsumer) {

    final var topic = "topic-42";
    final var partition = 42;
    final var offset = 142;
    final var partitions = new HashSet<TopicPartition>();
    final var topicPartition = new TopicPartition(topic, partition);
    partitions.add(topicPartition);

    final var mockConsumer = new MockConsumer<K, V>(OffsetResetStrategy.LATEST);
    mockConsumer.assign(partitions);

    final var consumer = KafkaConsumer.create(vertx, mockConsumer);

    final var offsetManager = new UnorderedConsumerOffsetManager<>(consumer);
    final var record = new KafkaConsumerRecordImpl<>(
        new ConsumerRecord<K, V>(
            topic,
            partition,
            offset,
            null,
            null
        )
    );

    rConsumer.accept(record, offsetManager);
    try {
      // commit happens asynchronously, which means this test could potentially fail, so let's sleep
      Thread.sleep(TIMEOUT_MS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final var committed = mockConsumer.committed(partitions);

    assertThat(committed).hasSize(1);
    assertThat(committed.keySet()).containsExactlyInAnyOrder(topicPartition);
    assertThat(committed.values()).containsExactlyInAnyOrder(new OffsetAndMetadata(offset, ""));
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void shouldNeverCommit(final KafkaConsumer<K, V> consumer) {
    verify(consumer, never()).commit();
    verify(consumer, never()).commit(any(Handler.class));
    verify(consumer, never()).commit(any(Map.class));
    verify(consumer, never()).commit(any(), any());
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void shouldNeverPause(final KafkaConsumer<K, V> consumer) {
    verify(consumer, never()).pause();
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class));
    verify(consumer, never()).pause(any(Set.class));
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class), any());
    verify(consumer, never()).pause(any(Set.class), any());
  }
}