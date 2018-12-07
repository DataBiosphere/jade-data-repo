package bio.terra.stairway;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Stairway class tests - mostly validating error conditions
public class StairwayTest {
    private Stairway stairway;
    private SafeHashMap safeHashMap;

    @Before
    public void setup() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        stairway = new Stairway(executorService, null, "test", true);
        safeHashMap = new SafeHashMap();
    }

    @Test(expected = MakeFlightException.class)
    public void testNullFlightClass() {
        stairway.submit(null, safeHashMap);
    }

    @Test(expected = MakeFlightException.class)
    public void testNullInputParams() {
        stairway.submit(TestFlight.class, null);
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadFlightDone() {
        stairway.isDone("abcdefg");
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadFlightGetResult() {
        stairway.getResult("abcdefg");
    }

}
