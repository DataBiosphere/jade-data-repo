package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayExecutionException;
import org.junit.Test;

public class FlightTest {

    @Test(expected = StairwayExecutionException.class)
    public void testInvalidStepIndex() {
        SafeHashMap sham = new SafeHashMap();
        Flight flight = new Flight(sham);
        flight.context().stepIndex(-5);
        flight.call();
    }

}
