package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.EventMatcherTest.TestCase;
import dev.knative.eventing.kafka.broker.core.proto.BrokersConfig.Trigger;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(value = ExecutionMode.CONCURRENT)
public class TriggerWrapperTest {

  @Test
  public void idCallShouldBeDelegatedToWrappedTrigger() {
    final var id = "123-42";
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder().setId(id).build()
    );

    assertThat(triggerWrapper.id()).isEqualTo(id);

  }

  @Test
  public void destinationCallShouldBeDelegatedToWrappedTrigger() {
    final var destination = "destination-42";
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder().setDestination(destination).build()
    );

    assertThat(triggerWrapper.destination()).isEqualTo(destination);
  }

  // test if filter returned by filter() agrees with EventMatcher
  @ParameterizedTest
  @MethodSource(value = "dev.knative.eventing.kafka.broker.core.EventMatcherTest#testCases")
  public void testFilter(final TestCase testCase) {
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder()
            .putAllAttributes(testCase.attributes)
            .build()
    );

    final var filter = triggerWrapper.filter();

    final var match = filter.match(testCase.event);

    assertThat(match).isEqualTo(testCase.shouldMatch);
  }

  /**
   * @return a stream of pairs that are semantically different.
   * @see dev.knative.eventing.kafka.broker.core.TriggerTest#testTriggerDifference(Entry)
   */
  @SuppressWarnings("rawtypes")
  public static Stream<Entry<dev.knative.eventing.kafka.broker.core.Trigger, dev.knative.eventing.kafka.broker.core.Trigger>> differentTriggersProvider() {
    return Stream.of(
        // trigger's destination is different
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Collections.emptyMap())
                .setDestination("this-is-my-destination1")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Collections.emptyMap())
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        // trigger's attributes are different
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion1",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value1"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        // trigger's id is different
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello1")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        )
    );
  }

  /**
   * @return a stream of pairs that are semantically equivalent.
   * @see dev.knative.eventing.kafka.broker.core.TriggerTest#testTriggerEquality(Entry)
   */
  @SuppressWarnings("rawtypes")
  public static Stream<Map.Entry<dev.knative.eventing.kafka.broker.core.Trigger, dev.knative.eventing.kafka.broker.core.Trigger>> equalTriggersProvider() {
    return Stream.of(
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .setId("1234")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .setId("1234")
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .setDestination("dest")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .setDestination("dest")
                .build()
            )
        ),
        new SimpleImmutableEntry<>(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type1"
                ))
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type1"
                ))
                .build()
            )
        )
    );
  }
}