package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class UpdateProfileRetrieveExistingProfileStep implements Step {

  private final ProfileService profileService;
  private final BillingProfileUpdateModel profileRequest;

  public UpdateProfileRetrieveExistingProfileStep(
      ProfileService profileService, BillingProfileUpdateModel profileRequest) {
    this.profileService = profileService;
    this.profileRequest = profileRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // get current billing model so we know what to revert to
    BillingProfileModel oldProfileModel =
        profileService.getProfileByIdNoCheck(profileRequest.getId());
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.REVERT_TO.getKeyName(), oldProfileModel);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
