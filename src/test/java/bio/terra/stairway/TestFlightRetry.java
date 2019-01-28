package bio.terra.stairway;

import org.apache.commons.lang3.StringUtils;

public class TestFlightRetry extends Flight {

    public TestFlightRetry(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        RetryRule retryRule;

        // Pull out our parameters and feed them in to the step classes.
        String retryType = inputParameters.get("retryType", String.class);
        Integer failCount = inputParameters.get("failCount", Integer.class);

        if (StringUtils.equals("fixed", retryType)) {
            Integer intervalSeconds = inputParameters.get("intervalSeconds", Integer.class);
            Integer maxCount = inputParameters.get("maxCount", Integer.class);
            retryRule = new RetryRuleFixedInterval(intervalSeconds, maxCount);
        } else if (StringUtils.equals("exponential", retryType)) {
            Long initialIntervalSeconds = inputParameters.get("initialIntervalSeconds", Long.class);
            Long maxIntervalSeconds = inputParameters.get("maxIntervalSeconds", Long.class);
            Long maxOperationTimeSeconds = inputParameters.get("maxOperationTimeSeconds", Long.class);
            retryRule = new RetryRuleExponentialBackoff(
                    initialIntervalSeconds,
                    maxIntervalSeconds,
                    maxOperationTimeSeconds);
        } else {
            throw new IllegalArgumentException("Invalid inputParameter retryType");
        }

        // Step 1 - test file existence
        addStep(new TestStepRetry(failCount), retryRule);
    }

}
