package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
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

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    // Registering the Managed Resource Group has no side effects to clean up
    return StepResult.getStepResultSuccess();
  }
}
