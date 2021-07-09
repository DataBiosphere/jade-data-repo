package bio.terra.flight;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.CloudPlatform;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.profile.flight.create.ProfileCreateFlight;
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
public class ProfileCreateFlightTest {

  @Mock private ApplicationContext context;

  @Test
  public void testConstructFlightAzure() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.AZURE);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, context);

    var packageName = "bio.terra.datarepo.service.profile.flight.create";
    assertThat(
        flight.context().getStepClassNames(),
        is(
            List.of(
                packageName + ".CreateProfileMetadataStep",
                packageName + ".CreateProfileVerifyAccountStep",
                packageName + ".CreateProfileVerifyDeployedApplicationStep",
                packageName + ".CreateProfileAuthzIamStep")));
  }

  @Test
  public void testConstructFlightGCP() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.GCP);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, context);

    var packageName = "bio.terra.datarepo.service.profile.flight.create";
    assertThat(
        flight.context().getStepClassNames(),
        is(
            List.of(
                packageName + ".CreateProfileMetadataStep",
                packageName + ".CreateProfileVerifyAccountStep",
                packageName + ".CreateProfileAuthzIamStep")));
  }
}
