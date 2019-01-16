package bio.terra.stairway;

public class RetryRuleNone implements RetryRule {
    private static RetryRuleNone retryRuleNoneSingleton = new RetryRuleNone();
    public static RetryRuleNone getRetryRuleNone() {
        return retryRuleNoneSingleton;
    }

    @Override
    public void initialize() {
    }

    @Override
    public boolean retrySleep() throws InterruptedException {
        return false;
    }
}
