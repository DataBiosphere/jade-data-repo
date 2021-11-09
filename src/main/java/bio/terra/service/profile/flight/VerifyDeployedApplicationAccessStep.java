package bio.terra.service.profile.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class VerifyDeployedApplicationAccessStep implements Step {

  private final ProfileService profileService;
  private final AuthenticatedUserRequest user;

  public VerifyDeployedApplicationAccessStep(
      ProfileService profileService, AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    profileService.verifyDeployedApplication(
        profileModel.getSubscriptionId(),
        profileModel.getResourceGroupName(),
        profileModel.getApplicationDeploymentName(),
        user);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Verify account has no side effects to clean up
    return StepResult.getStepResultSuccess();
  }
}
