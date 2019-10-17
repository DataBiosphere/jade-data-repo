package bio.terra.stairway;


import bio.terra.category.StairwayUnit;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(StairwayUnit.class)
public class RetryTest {
    private Stairway stairway;
    private UserRequestInfo testUser = new UserRequestInfo()
        .subjectId("StairwayUnit")
        .name("stairway@unit.com");

    @Autowired
    private StairwayJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        stairway = TestUtil.setupStairway(jdbcConfiguration);
    }

    @Test
    public void fixedSuccessTest() {
        // Fixed interval where maxCount > failCount should succeed
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("retryType", "fixed");
        inputParameters.put("failCount", Integer.valueOf(2));
        inputParameters.put("intervalSeconds", Integer.valueOf(2));
        inputParameters.put("maxCount", Integer.valueOf(4));

        String flightId = "successTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters, testUser);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        Assert.assertFalse(result.getException().isPresent());
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
        String flightId = "failureTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters, testUser);
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

        String flightId = "exponentialTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters, testUser);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        Assert.assertFalse(result.getException().isPresent());
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

        String flightId = "expOpTimeTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters, testUser);

        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
        Assert.assertTrue(result.getException().isPresent());
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
        String flightId = "expMaxTest";
        stairway.submit(flightId, TestFlightRetry.class, inputParameters, testUser);
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        LocalDateTime endTime = LocalDateTime.now();

        LocalDateTime startRange = startTime.plus(Duration.ofSeconds(14));
        LocalDateTime endRange = startTime.plus(Duration.ofSeconds(30));
        Assert.assertTrue(endTime.isAfter(startRange));
        Assert.assertTrue(endTime.isBefore(endRange));

        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.SUCCESS));
        Assert.assertFalse(result.getException().isPresent());
    }

}
