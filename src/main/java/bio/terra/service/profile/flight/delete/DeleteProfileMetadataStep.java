package bio.terra.service.profile.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteProfileMetadataStep implements Step {

  private final ProfileService profileService;
  private final UUID profileId;

  public DeleteProfileMetadataStep(ProfileService profileService, UUID profileId) {
    this.profileService = profileService;
    this.profileId = profileId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    final DeleteResponseModel.ObjectStateEnum stateEnum;
    if (profileService.deleteProfileMetadata(profileId)) {
      stateEnum = DeleteResponseModel.ObjectStateEnum.DELETED;
    } else {
      stateEnum = DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    }
    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
