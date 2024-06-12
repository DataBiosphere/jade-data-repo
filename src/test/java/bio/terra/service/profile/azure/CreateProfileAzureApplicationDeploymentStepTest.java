package bio.terra.service.profile.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.CreateProfileAzureApplicationDeploymentStep;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class CreateProfileAzureApplicationDeploymentStepTest {
  @Mock private AzureApplicationDeploymentService azureApplicationDeploymentService;
  @Mock private FlightContext flightContext;

  private FlightMap inputParameters;
  private BillingProfileModel billingProfile;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID BILLING_PROFILE_ID = UUID.randomUUID();
  private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
  private static final String RESOURCE_GROUP_NAME = "resourceGroupName";
  private static final String APPLICATION_DEPLOYMENT_ID =
      String.format("subscriptions/%s/resourceGroups/%s", SUBSCRIPTION_ID, RESOURCE_GROUP_NAME);

  private BillingProfileRequestModel request;
  private CreateProfileAzureApplicationDeploymentStep step;

  @BeforeEach
  void setup() {
    request = new BillingProfileRequestModel().id(BILLING_PROFILE_ID);
    step =
        new CreateProfileAzureApplicationDeploymentStep(
            azureApplicationDeploymentService, request, TEST_USER);

    billingProfile = new BillingProfileModel().id(BILLING_PROFILE_ID);

    inputParameters = new FlightMap();
    inputParameters.put(ProfileMapKeys.PROFILE_MODEL, billingProfile);

    AzureApplicationDeploymentResource azureApplicationDeploymentResource =
        new AzureApplicationDeploymentResource()
            .id(UUID.randomUUID())
            .azureApplicationDeploymentId(APPLICATION_DEPLOYMENT_ID);

    when(flightContext.getWorkingMap()).thenReturn(inputParameters);
    when(azureApplicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile))
        .thenReturn(azureApplicationDeploymentResource);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(azureApplicationDeploymentService).getOrRegisterApplicationDeployment(billingProfile);
    Assertions.assertTrue(
        inputParameters.containsKey(ProfileMapKeys.PROFILE_AZURE_APP_DEPLOYMENT_RESOURCE));
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}
