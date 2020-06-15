package dev.knative.eventing.kafka.broker.jmh;

import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;

public class ExampleBenchmark2 {

  @Benchmark
  public void init() {

  }

  public static void main(String[] args) throws IOException {
    org.openjdk.jmh.Main.main(args);
  }
}
