package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Optional;

public class CreateProfileVerifyAccountStep implements Step {
  private final ProfileService profileService;
  private final BillingProfileRequestModel request;
  private final AuthenticatedUserRequest user;
  private final Optional<String> idpAccessToken;

  public CreateProfileVerifyAccountStep(
      ProfileService profileService,
      BillingProfileRequestModel request,
      AuthenticatedUserRequest user,
      Optional<String> idpAccessToken) {
    this.profileService = profileService;
    this.request = request;
    this.user = user;
    this.idpAccessToken = idpAccessToken;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    profileService.verifyGoogleBillingAccount(request.getBillingAccountId(), user, idpAccessToken);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Verify account has no side effects to clean up
    return StepResult.getStepResultSuccess();
  }
}
