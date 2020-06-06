package dev.knative.eventing.kafka.broker.receiver;

import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.TOPIC_PREFIX;
import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.topic;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    assertEquals(
        TOPIC_PREFIX + "broker-namespace-broker-name",
        topic("/broker-namespace/broker-name")
    );
    assertEquals(
        TOPIC_PREFIX + "broker-namespace-broker-name",
        topic("/broker-namespace/broker-name/")
    );
  }

  @Test
  public void shouldReturnEmptyIfBrokerNamespaceAndBrokerNameAreFollowedBySomething() {
    assertNull(topic("/broker-namespace/broker-name/something"));
    assertNull(topic("/broker-namespace/broker-name/something/"));
  }
}