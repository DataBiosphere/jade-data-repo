package bio.terra.stairway;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RetryTest {
    private ExecutorService executorService;
    private Stairway stairway;

    @Before
    public void setup() {
        executorService = Executors.newFixedThreadPool(2);
        Stairway stairway = new Stairway(executorService);
    }

    @Test
    public void fixedSuccessTest() {
        // Fixed interval where maxCount > failCount should succeed
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", Integer.valueOf(2));
        inputParameters.put("intervalSeconds", Integer.valueOf(2));
        inputParameters.put("maxCount", Integer.valueOf(4));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        FlightResult result = stairway.getResult(flightId);
        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.getThrowable().isPresent());

        stairway.release(flightId);
    }

    @Test
    public void fixedFailureTest() {
        // Fixed interval where maxCount =< failCount should fail
        int intervalSeconds = 2;
        int maxCount = 3;

        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", Integer.valueOf(100));
        inputParameters.put("intervalSeconds", Integer.valueOf(intervalSeconds));
        inputParameters.put("maxCount", Integer.valueOf(maxCount));

        // fail time should be >= maxCount * intervalSeconds
        // and not too long... whatever that is. How about (maxCount+1 * intervalSeconds
        LocalDateTime startTime = LocalDateTime.now();
        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        FlightResult result = stairway.getResult(flightId);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(maxCount * intervalSeconds));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds((maxCount + 1) * intervalSeconds));
        Assert.assertTrue(endTime.isAfter(startRange));
        Assert.assertTrue(endTime.isBefore(endRange));
        Assert.assertFalse(result.isSuccess());
        stairway.release(flightId);
    }

    @Test
    public void exponentialSuccessTest() {
        // Exponential with generous limits
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(2));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(1));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(100));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(100));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        FlightResult result = stairway.getResult(flightId);

        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.getThrowable().isPresent());
        stairway.release(flightId);
    }

    @Test
    public void exponentialOpTimeFailureTest() {
        // Should fail by running out of operation time
        // Should go 2 + 4 + 8 + 16 - well over 10
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(4));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(2));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(100));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(10));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        FlightResult result = stairway.getResult(flightId);

        Assert.assertFalse(result.isSuccess());
        stairway.release(flightId);
    }

    @Test
    public void exponentialMaxIntervalSuccessTest() {
        // Should succeed in 4 tries. The time should be capped by
        // the maxInterval of 4. That is,
        // 2 + 4 + 4 + 4 = 14 should be less than 2 + 4 + 8 + 16 = 30
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(4));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(2));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(4));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(100));

        LocalDateTime startTime = LocalDateTime.now();
        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        FlightResult result = stairway.getResult(flightId);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(14));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds(30));
        Assert.assertTrue(endTime.isAfter(startRange));
        Assert.assertTrue(endTime.isBefore(endRange));

        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.getThrowable().isPresent());
        stairway.release(flightId);
    }

}
