package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import bio.terra.stairway.exception.StairwayExecutionException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(StairwayUnit.class)
public class FlightTest {

    @Test(expected = StairwayExecutionException.class)
    public void testInvalidStepIndex() {
        FlightMap sham = new FlightMap();
        Flight flight = new Flight(sham, null);
        flight.context().setStepIndex(-5);
        flight.call();
    }

}
