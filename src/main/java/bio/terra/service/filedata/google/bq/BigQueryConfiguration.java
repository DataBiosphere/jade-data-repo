package bio.terra.service.filedata.google.bq;

import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Configuration for interacting with BigQuery. */
@Configuration
@Profile("google")
@ConfigurationProperties(prefix = "datarepo.bq")
public class BigQueryConfiguration {

  private static final int DEFAULT_MAX_NUM_UPDATE_LIMIT_INDUCED_RETRIES = 3;
  private static final int DEFAULT_RETRY_WAIT_MS = 500;

  private Integer rateLimitRetries;
  private Integer rateLimitRetryWaitMs;

  public int getRateLimitRetries() {
    return Optional.ofNullable(rateLimitRetries)
        .orElse(DEFAULT_MAX_NUM_UPDATE_LIMIT_INDUCED_RETRIES);
  }

  public void setRateLimitRetries(Integer rateLimitRetries) {
    this.rateLimitRetries = rateLimitRetries;
  }

  public int getRateLimitRetryWaitMs() {
    return Optional.ofNullable(rateLimitRetryWaitMs).orElse(DEFAULT_RETRY_WAIT_MS);
  }

  public void setRateLimitRetryWaitMs(Integer rateLimitRetryWaitMs) {
    this.rateLimitRetryWaitMs = rateLimitRetryWaitMs;
  }
}
