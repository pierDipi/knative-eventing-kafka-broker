package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BrokerTest {

  @ParameterizedTest
  @MethodSource(value = {
      "dev.knative.eventing.kafka.broker.core.BrokerWrapperTest#equalTriggersProvider"
  })
  public void testTriggerEquality(final Map.Entry<Broker, Broker> entry) {
    assertThat(entry.getKey()).isEqualTo(entry.getValue());
    assertThat(entry.getKey().hashCode()).isEqualTo(entry.getValue().hashCode());
  }

  @ParameterizedTest
  @MethodSource(value = {
      "dev.knative.eventing.kafka.broker.core.BrokerWrapperTest#differentTriggersProvider"
  })
  public void testTriggerDifference(final Map.Entry<Broker, Broker> entry) {
    assertThat(entry.getKey()).isNotEqualTo(entry.getValue());
    assertThat(entry.getKey().hashCode()).isNotEqualTo(entry.getValue().hashCode());
  }
}