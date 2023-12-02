package bio.terra.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.stairway.FlightMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ProfileDeleteFlightTest {

  @Mock private ApplicationContext context;

  @Test
  public void testConstructFlight() {
    var flight = new ProfileDeleteFlight(new FlightMap(), context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "DeleteProfileMarkUnusedProjects",
            "DeleteProfileDeleteUnusedProjects",
            "DeleteProfileProjectMetadata",
            "DeleteProfileMetadataStep",
            "DeleteProfileAuthzIamStep",
            "JournalRecordDeleteEntryStep"));
  }
}
