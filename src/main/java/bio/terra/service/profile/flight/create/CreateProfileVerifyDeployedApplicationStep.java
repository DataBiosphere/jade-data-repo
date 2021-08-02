package bio.terra.service.profile.flight.create;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateProfileVerifyDeployedApplicationStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateProfileVerifyDeployedApplicationStep.class);

  private final ProfileService profileService;
  private final BillingProfileRequestModel request;
  private final AuthenticatedUserRequest user;

  public CreateProfileVerifyDeployedApplicationStep(
      ProfileService profileService,
      BillingProfileRequestModel request,
      AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.request = request;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    profileService.verifyDeployedApplication(
        request.getSubscriptionId(),
        request.getResourceGroupName(),
        request.getApplicationDeploymentName(),
        user);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Verify account has no side effects to clean up
    return StepResult.getStepResultSuccess();
  }
}
