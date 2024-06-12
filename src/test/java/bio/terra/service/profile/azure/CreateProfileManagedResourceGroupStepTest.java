package bio.terra.service.profile.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.CreateProfileManagedResourceGroup;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class CreateProfileManagedResourceGroupStepTest {
  @Mock private ProfileService profileService;
  @Mock private FlightContext flightContext;
  private FlightMap inputParameters;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID BILLING_PROFILE_ID = UUID.randomUUID();
  private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
  private static final String RESOURCE_GROUP_NAME = "resourceGroupName";
  private static final String APPLICATION_DEPLOYMENT_ID =
      String.format("subscriptions/%s/resourceGroups/%s", SUBSCRIPTION_ID, RESOURCE_GROUP_NAME);

  private BillingProfileRequestModel request;
  private CreateProfileManagedResourceGroup step;

  @BeforeEach
  void setup() {
    request = new BillingProfileRequestModel().id(BILLING_PROFILE_ID);
    step = new CreateProfileManagedResourceGroup(profileService, request, TEST_USER);

    AzureApplicationDeploymentResource azureApplicationDeploymentResource =
        new AzureApplicationDeploymentResource()
            .azureApplicationDeploymentId(APPLICATION_DEPLOYMENT_ID)
            .azureResourceGroupName(RESOURCE_GROUP_NAME);

    inputParameters = new FlightMap();
    inputParameters.put(
        ProfileMapKeys.PROFILE_AZURE_APP_DEPLOYMENT_RESOURCE, azureApplicationDeploymentResource);

    when(flightContext.getWorkingMap()).thenReturn(inputParameters);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(profileService).registerManagedResourceGroup(request, TEST_USER, RESOURCE_GROUP_NAME);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(profileService).deregisterManagedResourceGroup(request, TEST_USER);
  }
}
