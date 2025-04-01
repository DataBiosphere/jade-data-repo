package bio.terra.service.profile.flight;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class LocateBillingInfoStep extends DefaultUndoStep {
  private final ProfileService profileService;
  private final UUID profileId;

  public LocateBillingInfoStep(ProfileService profileService, UUID profileId) {
    this.profileService = profileService;
    this.profileId = profileId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    // Not auth check: Just a check if there is an entry in our db for this billing profile
    try {
      profileService.getProfileByIdNoCheck(profileId);
      workingMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, true);
    } catch (ProfileNotFoundException ex) {
      // Assume that the billing project lives in Rawls
      // AuthorizeRawlsBillingProjectsUseStep will return the relevant error message if not
      workingMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, false);
    }
    return StepResult.getStepResultSuccess();
  }
}
