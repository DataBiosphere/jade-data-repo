package bio.terra.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
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
public class ProfileCreateFlightTest {

  @Mock private ApplicationContext context;

  @Test
  public void testConstructFlightAzure() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.AZURE);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, context);

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
  public void testConstructFlightGCP() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.GCP);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, context);

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
