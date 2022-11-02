package bio.terra.flight;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.stream.Collectors;
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
public class ProfileUpdateFlightTest {

  @Mock private ApplicationContext context;
  @Mock private FlightMap map;
  @Mock private bio.terra.model.BillingProfileUpdateModel profileMock;

  @Test
  public void testConstructFlight() {
    when(map.get(JobMapKeys.REQUEST.getKeyName(), bio.terra.model.BillingProfileUpdateModel.class))
        .thenReturn(profileMock);
    var flight = new ProfileUpdateFlight(map, context);
    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getSimpleName())
            .collect(Collectors.toList());
    assertThat(
        steps,
        is(
            List.of(
                "UpdateProfileRetrieveExistingProfileStep",
                "UpdateProfileMetadataStep",
                "UpdateProfileVerifyAccountStep",
                "UpdateProfileUpdateGCloudProject",
                "JournalRecordUpdateEntryStep")));
  }
}
