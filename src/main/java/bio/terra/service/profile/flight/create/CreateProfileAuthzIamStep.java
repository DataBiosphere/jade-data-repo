package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateProfileAuthzIamStep implements Step {
  private final ProfileService profileService;
  private final BillingProfileRequestModel request;
  private final AuthenticatedUserRequest user;

  public CreateProfileAuthzIamStep(
      ProfileService profileService,
      BillingProfileRequestModel request,
      AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.request = request;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    profileService.createProfileIamResource(request, user);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    profileService.deleteProfileIamResource(request.getId(), user);
    return StepResult.getStepResultSuccess();
  }
}
