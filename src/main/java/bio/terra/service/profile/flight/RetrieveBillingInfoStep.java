package bio.terra.service.profile.flight;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.rawls.RawlsService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class RetrieveBillingInfoStep extends DefaultUndoStep {

  private final RawlsService rawlsService;
  private final ProfileService profileService;
  private final UUID profileId;
  private final AuthenticatedUserRequest user;

  public RetrieveBillingInfoStep(RawlsService rawlsService,
      ProfileService profileService, UUID profileId, AuthenticatedUserRequest user) {
    this.rawlsService = rawlsService;
    this.profileService = profileService;
    this.profileId = profileId;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    try {
      BillingProfileModel profileModel = profileService.getProfileByIdNoCheck(profileId);
      if (profileModel == null) {
        throw new ProfileNotFoundException("Profile not found");
      }
      workingMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, true);
    } catch (ProfileNotFoundException ex) {
      // check rawls for the profile
      try {
        // TODO - how can make sure we pass through the most helpful error to the user?
        var rawlsProjectExists = rawlsService.rawlsBillingProjectExists(profileId, user);
        if (rawlsProjectExists) {
          workingMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, false);
        } else {
          return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new ProfileNotFoundException("Profile not found"));
        }
      } catch (Exception e) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
