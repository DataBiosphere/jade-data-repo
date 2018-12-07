package bio.terra.stairway;

public class RetryRuleNone implements RetryRule {
    static RetryRuleNone retryRuleNone;
    static {
        retryRuleNone = new RetryRuleNone();
    }

    @Override
    public void initialize() {
    }

    @Override
    public boolean retrySleep() throws InterruptedException {
        return false;
    }
}
