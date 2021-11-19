package bio.terra.flight;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.stream.Collectors;
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

    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getSimpleName())
            .collect(Collectors.toList());
    assertThat(
        steps,
        is(
            List.of(
                "CreateProfileMetadataStep",
                "CreateProfileVerifyDeployedApplicationStep",
                "CreateProfileAuthzIamStep")));
  }

  @Test
  public void testConstructFlightGCP() {
    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setCloudPlatform(CloudPlatform.GCP);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), billingProfileRequestModel);

    var flight = new ProfileCreateFlight(inputParameters, context);

    var packageName = "bio.terra.service.profile.flight.create";
    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getName())
            .collect(Collectors.toList());
    assertThat(
        steps,
        is(
            List.of(
                packageName + ".CreateProfileMetadataStep",
                packageName + ".CreateProfileVerifyAccountStep",
                packageName + ".CreateProfileAuthzIamStep")));
  }
}
