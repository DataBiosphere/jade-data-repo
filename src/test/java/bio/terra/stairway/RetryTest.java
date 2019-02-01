package bio.terra.stairway;


import bio.terra.category.StairwayUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

@Category(StairwayUnit.class)
public class RetryTest {
    private ExecutorService executorService;
    private Stairway stairway;

    @Before
    public void setup() {
        executorService = Executors.newFixedThreadPool(2);
        stairway = new Stairway(executorService);
    }

    @Test
    public void fixedSuccessTest() {
        // Fixed interval where maxCount > failCount should succeed
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", Integer.valueOf(2));
        inputParameters.put("intervalSeconds", Integer.valueOf(2));
        inputParameters.put("maxCount", Integer.valueOf(4));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        Assert.assertFalse(result.getErrorMessage().isPresent());
    }

    @Test
    public void fixedFailureTest() {
        // Fixed interval where maxCount =< failCount should fail
        int intervalSeconds = 2;
        int maxCount = 3;

        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", Integer.valueOf(100));
        inputParameters.put("intervalSeconds", Integer.valueOf(intervalSeconds));
        inputParameters.put("maxCount", Integer.valueOf(maxCount));

        // fail time should be >= maxCount * intervalSeconds
        // and not too long... whatever that is. How about (maxCount+1 * intervalSeconds
        LocalDateTime startTime = LocalDateTime.now();
        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(maxCount * intervalSeconds));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds((maxCount + 1) * intervalSeconds));
        Assert.assertTrue(endTime.isAfter(startRange));
        Assert.assertTrue(endTime.isBefore(endRange));
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));
    }

    @Test
    public void exponentialSuccessTest() {
        // Exponential with generous limits
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(2));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(1));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(100));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(100));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        Assert.assertFalse(result.getErrorMessage().isPresent());
    }

    @Test
    public void exponentialOpTimeFailureTest() {
        // Should fail by running out of operation time
        // Should go 2 + 4 + 8 + 16 - well over 10
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(4));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(2));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(100));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(10));

        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);

        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
        Assert.assertTrue(result.getErrorMessage().isPresent());
    }

    @Test
    public void exponentialMaxIntervalSuccessTest() {
        // Should succeed in 4 tries. The time should be capped by
        // the maxInterval of 4. That is,
        // 2 + 4 + 4 + 4 = 14 should be less than 2 + 4 + 8 + 16 = 30
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "exponential");
        inputParameters.put("failCount", Integer.valueOf(4));
        inputParameters.put("initialIntervalSeconds", Long.valueOf(2));
        inputParameters.put("maxIntervalSeconds", Long.valueOf(4));
        inputParameters.put("maxOperationTimeSeconds", Long.valueOf(100));

        LocalDateTime startTime = LocalDateTime.now();
        String flightId = stairway.submit(TestFlightRetry.class, inputParameters);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(14));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds(30));
        Assert.assertTrue(endTime.isAfter(startRange));
        Assert.assertTrue(endTime.isBefore(endRange));

        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.SUCCESS));
        Assert.assertFalse(result.getErrorMessage().isPresent());
    }

}
