package bio.terra.service.profile.flight.update;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Optional;

public class UpdateProfileVerifyAccountStep implements Step {
  private final ProfileService profileService;
  private final AuthenticatedUserRequest user;
  private final Optional<String> idpAccessToken;

  public UpdateProfileVerifyAccountStep(
      ProfileService profileService,
      AuthenticatedUserRequest user,
      Optional<String> idpAccessToken) {
    this.profileService = profileService;
    this.user = user;
    this.idpAccessToken = idpAccessToken;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel newBillingProfileModel =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), BillingProfileModel.class);
    profileService.verifyGoogleBillingAccount(
        newBillingProfileModel.getBillingAccountId(), user, idpAccessToken);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
