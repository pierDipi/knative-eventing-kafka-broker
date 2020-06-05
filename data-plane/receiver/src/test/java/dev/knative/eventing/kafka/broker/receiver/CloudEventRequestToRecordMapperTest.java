package dev.knative.eventing.kafka.broker.receiver;

import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.topic;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class CloudEventRequestToRecordMapperTest {

  @Test
  public void shouldReturnEmptyTopicIfRootPath() {
    assertNull(topic("/"));
    assertNull(topic(""));
  }

  @Test
  public void shouldReturnEmptyTopicIfNoBrokerName() {
    assertNull(topic("/broker-namespace"));
    assertNull(topic("/broker-namespace/"));
  }

  @Test
  public void shouldReturnTopicNameIfCorrectPath() {
    assertNotNull(topic("/broker-namespace/broker-name"));
    assertNotNull(topic("/broker-namespace/broker-name/"));
  }

  @Test
  public void shouldReturnEmptyIfBrokerNamespaceAndBrokerNameAreFollowedBySomething() {
    assertNull(topic("/broker-namespace/broker-name/something"));
    assertNull(topic("/broker-namespace/broker-name/something/"));
  }
}