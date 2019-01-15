package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayExecutionException;
import org.junit.Test;

public class FlightTest {

    @Test(expected = StairwayExecutionException.class)
    public void testInvalidStepIndex() {
        FlightMap sham = new FlightMap();
        Flight flight = new Flight(sham);
        flight.context().setStepIndex(-5);
        flight.call();
    }

}
