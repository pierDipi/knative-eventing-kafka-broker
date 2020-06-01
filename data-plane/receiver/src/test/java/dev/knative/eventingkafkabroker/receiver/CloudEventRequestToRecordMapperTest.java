package dev.knative.eventingkafkabroker.receiver;

import static dev.knative.eventingkafkabroker.receiver.CloudEventRequestToRecordMapper.topic;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class CloudEventRequestToRecordMapperTest {

  @Test
  public void shouldReturnEmptyTopicIfRootPath() {
    assertTrue(topic("/").isEmpty());
    assertTrue(topic("").isEmpty());
  }

  @Test
  public void shouldReturnEmptyTopicIfNoBrokerName() {
    assertTrue(topic("/broker-namespace").isEmpty());
    assertTrue(topic("/broker-namespace/").isEmpty());
  }

  @Test
  public void shouldReturnTopicNameIfCorrectPath() {
    assertTrue(topic("/broker-namespace/broker-name").isPresent());
    assertTrue(topic("/broker-namespace/broker-name/").isPresent());
  }

  @Test
  public void shouldReturnEmptyIfBrokerNamespaceAndBrokerNameAreFollowedBySomething() {
    assertTrue(topic("/broker-namespace/broker-name/something").isEmpty());
    assertTrue(topic("/broker-namespace/broker-name/something/").isEmpty());
  }
}