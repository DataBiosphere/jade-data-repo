package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Stairway class tests - mostly validating error conditions
@Category(StairwayUnit.class)
public class StairwayTest {
    private Stairway stairway;
    private FlightMap flightMap;

    @Before
    public void setup() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        stairway = new Stairway(executorService);
        flightMap = new FlightMap();
    }

    @Test(expected = MakeFlightException.class)
    public void testNullFlightClass() {
        stairway.submit(null, flightMap);
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
        stairway.getFlightState("abcdefg");
    }

}
