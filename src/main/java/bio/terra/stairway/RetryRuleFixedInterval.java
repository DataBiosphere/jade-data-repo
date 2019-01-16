package bio.terra.stairway;

import java.util.concurrent.TimeUnit;

public class RetryRuleFixedInterval implements RetryRule {
    // Fixed parameters
    private int intervalSeconds;
    private int maxCount;

    // Initialized parameters
    private int retryCount;

    /**
     * Sleep for fixed intervals a fixed number of times.
     *
     * @param intervalSeconds - number of seconds to sleep
     * @param maxCount - number of times to retry
     */
    public RetryRuleFixedInterval(int intervalSeconds, int maxCount) {
        this.intervalSeconds = intervalSeconds;
        this.maxCount = maxCount;
        initialize();
    }

    @Override
    public void initialize() {
        retryCount = 0;
    }

    @Override
    public boolean retrySleep() throws InterruptedException {
        if (retryCount >= maxCount) {
            return false;
        }

        TimeUnit.SECONDS.sleep(intervalSeconds);
        retryCount++;
        return true;
    }
}
