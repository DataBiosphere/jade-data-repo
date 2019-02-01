package bio.terra.stairway;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import static bio.terra.stairway.Data.dubValue;
import static bio.terra.stairway.Data.errString;
import static bio.terra.stairway.Data.fkey;
import static bio.terra.stairway.Data.flightId;
import static bio.terra.stairway.Data.ikey;
import static bio.terra.stairway.Data.intValue;
import static bio.terra.stairway.Data.skey;
import static bio.terra.stairway.Data.strValue;
import static org.hamcrest.CoreMatchers.is;

public class FlightStateTest {
    private static String bad = "bad bad bad";
    private FlightState result;
    private Timestamp timestamp;

    @Before
    public void setup() {
        FlightMap inputs = new FlightMap();
        inputs.put(ikey, intValue);
        inputs.put(skey, strValue);
        inputs.put(fkey, dubValue);

        FlightMap outputs = new FlightMap();
        outputs.put(ikey, intValue);
        outputs.put(skey, strValue);
        outputs.put(fkey, dubValue);

        timestamp = Timestamp.from(Instant.now());

        result = new FlightState();
        result.setFlightId(flightId);
        result.setFlightStatus(FlightStatus.FATAL);
        result.setInputParameters(inputs);
        result.setSubmitted(timestamp);
        result.setCompleted(Optional.of(timestamp));
        result.setResultMap(Optional.of(outputs));
        result.setErrorMessage(Optional.of(errString));
    }

    @Test
    public void testFlightResultAccess() {
        Assert.assertThat(result.getFlightId(), is(flightId));
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.FATAL));
        Assert.assertThat(result.getInputParameters().get(skey, String.class), is(strValue));
        Assert.assertThat(result.getSubmitted(), is(timestamp));

        Assert.assertTrue(result.getCompleted().isPresent());
        Assert.assertTrue(result.getResultMap().isPresent());
        Assert.assertTrue(result.getErrorMessage().isPresent());

        Assert.assertThat(result.getCompleted().get(), is(timestamp));
        Assert.assertThat(result.getErrorMessage().get(), is(errString));

        FlightMap outputMap = result.getResultMap().get();
        Assert.assertThat(outputMap.get(fkey, Double.class), is(dubValue));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testResultMapIsImmutable() {
        result.getResultMap().get().put(bad, bad);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInputMapIsImmutable() {
        result.getInputParameters().put(bad, bad);
    }

}
