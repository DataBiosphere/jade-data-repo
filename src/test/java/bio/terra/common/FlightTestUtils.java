package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.stairway.Flight;
import bio.terra.stairway.Step;
import java.util.List;

public class FlightTestUtils {

  public static List<String> getStepNames(Flight flight) {
    return flight.getSteps().stream().map(step -> step.getClass().getSimpleName()).toList();
  }

  /** Assert that exactly one step of the desired class exists in the flight and return it. */
  public static <T> T getStepWithClass(Flight flight, Class<T> clazz) {
    List<Step> stepsWithClass = flight.getSteps().stream().filter(clazz::isInstance).toList();
    assertThat(
        "Flight has exactly one %s step".formatted(clazz.getSimpleName()),
        stepsWithClass,
        hasSize(1));
    return clazz.cast(stepsWithClass.get(0));
  }
}
