package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.stairway.Flight;
import bio.terra.stairway.Step;
import java.util.List;
import org.springframework.context.ApplicationContext;

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

  public static void mockFlightSetup(ApplicationContext context) {
    ApplicationConfiguration appConfig = mock(ApplicationConfiguration.class);
    when(appConfig.getMaxStairwayThreads()).thenReturn(1);
    when(context.getBean(any(Class.class))).thenReturn(null);
    // Beans that are interacted with directly in flight construction rather than simply passed
    // to steps need to be added to our context mock.
    when(context.getBean(ApplicationConfiguration.class)).thenReturn(appConfig);
  }
}
