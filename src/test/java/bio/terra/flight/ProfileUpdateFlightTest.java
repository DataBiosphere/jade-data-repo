package bio.terra.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.context.ApplicationContext;

@Tag(Unit.TAG)
class ProfileUpdateFlightTest {

  @Mock private FlightMap map;

  @Test
  void testConstructFlight() {
    FlightMap map = new FlightMap();
    map.put(JobMapKeys.REQUEST.getKeyName(), new BillingProfileUpdateModel());
    var flight = new ProfileUpdateFlight(map, mock(ApplicationContext.class));
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "UpdateProfileRetrieveExistingProfileStep",
            "UpdateProfileMetadataStep",
            "UpdateProfileVerifyAccountStep",
            "UpdateProfileUpdateGCloudProject",
            "JournalRecordUpdateEntryStep"));
  }
}
