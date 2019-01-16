package bio.terra.stairway;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class RetryRuleExponentialBackoff implements RetryRule {
    private long initialIntervalSeconds;
    private long maxIntervalSeconds;
    private long maxOperationTimeSeconds;

    private LocalDateTime endTime;
    private long intervalSeconds;

    /**
     * Retry with exponential backoff
     * @param initialIntervalSeconds - starting interval; will double up to max interval
     * @param maxIntervalSeconds - maximum interval to ever sleep
     * @param maxOperationTimeSeconds - maximum amount of time to allow for the operation
     */
    public RetryRuleExponentialBackoff(long initialIntervalSeconds,
                                       long maxIntervalSeconds,
                                       long maxOperationTimeSeconds) {
        this.initialIntervalSeconds = initialIntervalSeconds;
        this.maxIntervalSeconds = maxIntervalSeconds;
        this.maxOperationTimeSeconds = maxOperationTimeSeconds;
    }

    @Override
    public void initialize() {
        intervalSeconds = initialIntervalSeconds;
        endTime = LocalDateTime.now().plus(Duration.ofSeconds(maxOperationTimeSeconds));
    }

    @Override
    public boolean retrySleep() throws InterruptedException {
        if (LocalDateTime.now().isAfter(endTime)) {
            return false;
        }

        TimeUnit.SECONDS.sleep(intervalSeconds);
        intervalSeconds = intervalSeconds + intervalSeconds;
        if (intervalSeconds > maxIntervalSeconds) {
            intervalSeconds = maxIntervalSeconds;
        }
        return true;
    }
}
