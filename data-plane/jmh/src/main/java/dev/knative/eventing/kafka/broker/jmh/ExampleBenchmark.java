package dev.knative.eventing.kafka.broker.jmh;

import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;

public class ExampleBenchmark {

  @Benchmark
  public void bench() throws InterruptedException {
    Thread.sleep(1000);
  }

  public static void main(String[] args) throws IOException {
    org.openjdk.jmh.Main.main(args);
  }
}
