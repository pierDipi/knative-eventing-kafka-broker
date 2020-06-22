package dev.knative.eventing.kafka.broker.core;

/**
 * Trigger interface represents the Trigger object.
 *
 * <p>Each implementation must override: equals(object) and hashCode(), and those implementation
 * must catch Trigger updates (e.g. it's not safe to compare only the Trigger UID). It's recommended
 * to not relying on equals(object) and hashCode() generated by Protocol Buffer compiler.
 *
 * <p>Testing equals(object) and hashCode() of newly added implementation is done by adding sources
 * to parameterized tests in TriggerTest.
 */
public interface Trigger<T> {

  /**
   * Get trigger id.
   *
   * @return trigger identifier.
   */
  String id();

  /**
   * Get the filter.
   *
   * @return filter to use.
   */
  Filter<T> filter();

  /**
   * Get trigger destination URI.
   *
   * @return destination URI.
   */
  String destination();
}