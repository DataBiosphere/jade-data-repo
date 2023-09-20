package bio.terra.service.filedata.google.bq;

import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.context.annotation.Profile;

/** Configuration for interacting with BigQuery. */
@ConstructorBinding
@Profile("google")
@ConfigurationProperties(prefix = "datarepo.bq")
public record BigQueryConfiguration(Integer rateLimitRetries, Integer rateLimitRetryWaitMs) {

  private static final int DEFAULT_MAX_NUM_UPDATE_LIMIT_INDUCED_RETRIES = 3;
  private static final int DEFAULT_RETRY_WAIT_MS = 500;

  public int getRateLimitRetries() {
    return Optional.ofNullable(rateLimitRetries)
        .orElse(DEFAULT_MAX_NUM_UPDATE_LIMIT_INDUCED_RETRIES);
  }

  public int getRateLimitRetryWaitMs() {
    return Optional.ofNullable(rateLimitRetryWaitMs).orElse(DEFAULT_RETRY_WAIT_MS);
  }
}
