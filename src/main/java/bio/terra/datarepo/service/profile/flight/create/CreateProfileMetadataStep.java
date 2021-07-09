package bio.terra.datarepo.service.profile.flight.create;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class CreateProfileMetadataStep implements Step {

  private final ProfileService profileService;
  private final BillingProfileRequestModel profileRequest;
  private final AuthenticatedUserRequest user;
  private static final Logger logger = LoggerFactory.getLogger(CreateProfileMetadataStep.class);

  public CreateProfileMetadataStep(
      ProfileService profileService,
      BillingProfileRequestModel profileRequest,
      AuthenticatedUserRequest user) {
    this.profileService = profileService;
    this.profileRequest = profileRequest;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    BillingProfileModel profileModel = profileService.createProfileMetadata(profileRequest, user);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), profileModel);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    logger.debug("Profile creation failed. Deleting metadata.");
    profileService.deleteProfileMetadata(profileRequest.getId());
    return StepResult.getStepResultSuccess();
  }
}
