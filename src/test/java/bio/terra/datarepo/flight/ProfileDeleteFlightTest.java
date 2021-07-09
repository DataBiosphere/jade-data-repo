package bio.terra.flight;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.stairway.FlightMap;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Category(Unit.class)
public class ProfileDeleteFlightTest {

  @Mock private ApplicationContext context;

  @Test
  public void testConstructFlight() {
    var flight = new ProfileDeleteFlight(new FlightMap(), context);

    var packageName = "bio.terra.service.profile.flight.delete";

    assertThat(
        flight.context().getStepClassNames(),
        is(
            List.of(
                packageName + ".DeleteProfileMarkUnusedProjects",
                packageName + ".DeleteProfileDeleteUnusedProjects",
                packageName + ".DeleteProfileProjectMetadata",
                packageName + ".DeleteProfileMetadataStep",
                packageName + ".DeleteProfileAuthzIamStep")));
  }
}
