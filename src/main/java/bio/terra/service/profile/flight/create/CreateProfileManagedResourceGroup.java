package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateProfileManagedResourceGroup implements Step {

  private final ProfileService profileService;
  private final BillingProfileRequestModel request;
  private final AuthenticatedUserRequest user;

  public CreateProfileManagedResourceGroup(
      ProfileService profileService,
      BillingProfileRequestModel request,
      AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.request = request;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    AzureApplicationDeploymentResource azureApplicationDeploymentResource =
        workingMap.get(
            ProfileMapKeys.PROFILE_AZURE_APP_DEPLOYMENT_RESOURCE,
            AzureApplicationDeploymentResource.class);
    String azureResourceGroupName = azureApplicationDeploymentResource.getAzureResourceGroupName();
    profileService.registerManagedResourceGroup(request, user, azureResourceGroupName);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    profileService.deregisterManagedResourceGroup(request, user);
    return StepResult.getStepResultSuccess();
  }
}
