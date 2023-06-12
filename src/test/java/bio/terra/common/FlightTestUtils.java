package bio.terra.common;

import bio.terra.stairway.Flight;
import java.util.List;

public class FlightTestUtils {

  public static List<String> getStepNames(Flight flight) {
    return flight.getSteps().stream().map(step -> step.getClass().getSimpleName()).toList();
  }

  public static <T> List<T> getStepsWithClass(Flight flight, Class<T> clazz) {
    return flight.getSteps().stream()
        .filter(step -> clazz.isInstance(step))
        .map(step -> clazz.cast(step))
        .toList();
  }
}
