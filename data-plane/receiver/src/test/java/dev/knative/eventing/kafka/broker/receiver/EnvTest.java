package dev.knative.eventing.kafka.broker.receiver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class EnvTest {

  private static final String PORT = "8080";
  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final String PRODUCER_CONFIG_PATH = "/etc/producer";

  @Test
  public void create() {
    final var env = new Env(
        key -> {
          switch (key) {
            case Env.INGRESS_PORT:
              return PORT;
            case Env.LIVENESS_PROBE_PATH:
              return LIVENESS_PATH;
            case Env.READINESS_PROBE_PATH:
              return READINESS_PATH;
            case Env.PRODUCER_CONFIG_FILE_PATH:
              return PRODUCER_CONFIG_PATH;
            default:
              throw new IllegalArgumentException();
          }
        }
    );

    assertEquals(Integer.parseInt(PORT), env.getIngressPort());
    assertEquals(LIVENESS_PATH, env.getLivenessProbePath());
    assertEquals(READINESS_PATH, env.getReadinessProbePath());
    assertEquals(PRODUCER_CONFIG_PATH, env.getProducerConfigFilePath());
    assertFalse(env.toString().contains("@"));
  }
}