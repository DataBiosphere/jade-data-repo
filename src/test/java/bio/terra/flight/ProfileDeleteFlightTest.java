package bio.terra.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

@Tag(Unit.TAG)
class ProfileDeleteFlightTest {

  @Test
  void testConstructFlight() {
    var flight = new ProfileDeleteFlight(new FlightMap(), mock(ApplicationContext.class));
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
