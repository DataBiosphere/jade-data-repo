package bio.terra.common;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.Flight;
import java.util.List;

public class FlightTestUtils {

  public static List<String> getStepNames(Flight flight) {
    return flight.getSteps().stream().map(step -> step.getClass().getSimpleName()).toList();
  }
}
