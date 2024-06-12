package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateProfileAzureApplicationDeploymentStep implements Step {

  private final AzureApplicationDeploymentService azureApplicationDeploymentService;
  private final BillingProfileRequestModel request;
  private final AuthenticatedUserRequest user;

  public CreateProfileAzureApplicationDeploymentStep(
      AzureApplicationDeploymentService azureApplicationDeploymentService,
      BillingProfileRequestModel request,
      AuthenticatedUserRequest user) {
    this.azureApplicationDeploymentService = azureApplicationDeploymentService;
    this.request = request;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureApplicationDeploymentResource azureAppDeploymentResource =
        azureApplicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile);
    workingMap.put(
        ProfileMapKeys.PROFILE_AZURE_APP_DEPLOYMENT_RESOURCE, azureAppDeploymentResource);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
