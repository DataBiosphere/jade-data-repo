package bio.terra.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.stairway.FlightMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class ProfileCreateFlightTest {

  @Test
  void testConstructFlightAzure() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.AZURE);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, mock(ApplicationContext.class));

    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "GetOrCreateProfileIdStep",
            "CreateProfileMetadataStep",
            "CreateProfileVerifyDeployedApplicationStep",
            "CreateProfileAuthzIamStep",
            "CreateProfileJournalEntryStep"));
  }

  @Test
  void testConstructFlightGCP() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.GCP);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, mock(ApplicationContext.class));

    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "GetOrCreateProfileIdStep",
            "CreateProfileMetadataStep",
            "CreateProfileVerifyAccountStep",
            "CreateProfileAuthzIamStep",
            "CreateProfileJournalEntryStep"));
  }
}
