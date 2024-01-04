package bio.terra.service.profile.flight.update;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class UpdateProfileMetadataStep implements Step {

  private final ProfileService profileService;
  private final BillingProfileUpdateModel profileRequest;
  private final AuthenticatedUserRequest user;
  private static final Logger logger = LoggerFactory.getLogger(UpdateProfileMetadataStep.class);

  public UpdateProfileMetadataStep(
      ProfileService profileService,
      BillingProfileUpdateModel profileRequest,
      AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.profileRequest = profileRequest;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    BillingProfileModel profileModel = profileService.updateProfileMetadata(profileRequest);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), profileModel);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    logger.error("Profile update failed. Reverting to old profile metadata.");
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel oldProfileModel =
        workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);
    BillingProfileUpdateModel requestOldModel =
        new BillingProfileUpdateModel()
            .id(oldProfileModel.getId())
            .billingAccountId(oldProfileModel.getBillingAccountId())
            .description(oldProfileModel.getDescription());
    profileService.updateProfileMetadata(requestOldModel);
    return StepResult.getStepResultSuccess();
  }
}
