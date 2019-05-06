package bio.terra.stairway;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static bio.terra.stairway.TestUtil.dubValue;
import static bio.terra.stairway.TestUtil.errString;
import static bio.terra.stairway.TestUtil.fkey;
import static bio.terra.stairway.TestUtil.flightId;
import static bio.terra.stairway.TestUtil.ikey;
import static bio.terra.stairway.TestUtil.intValue;
import static bio.terra.stairway.TestUtil.skey;
import static bio.terra.stairway.TestUtil.strValue;
import static org.hamcrest.CoreMatchers.is;

public class FlightStateTest {
    private static String bad = "bad bad bad";
    private FlightState result;
    private Instant timestamp;

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

        timestamp = Instant.now();

        result = new FlightState();
        result.setFlightId(flightId);
        result.setFlightStatus(FlightStatus.FATAL);
        result.setInputParameters(inputs);
        result.setSubmitted(timestamp);
        result.setCompleted(Optional.of(timestamp));
        result.setResultMap(Optional.of(outputs));
        result.setException(Optional.of(new RuntimeException(errString)));
    }

    @Test
    public void testFlightResultAccess() {
        Assert.assertThat(result.getFlightId(), is(flightId));
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.FATAL));
        Assert.assertThat(result.getInputParameters().get(skey, String.class), is(strValue));
        Assert.assertThat(result.getSubmitted(), is(timestamp));

        Assert.assertTrue(result.getCompleted().isPresent());
        Assert.assertTrue(result.getResultMap().isPresent());
        Assert.assertTrue(result.getException().isPresent());

        Assert.assertThat(result.getCompleted().get(), is(timestamp));
        Assert.assertThat(result.getException().get().getMessage(), is(errString));

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
