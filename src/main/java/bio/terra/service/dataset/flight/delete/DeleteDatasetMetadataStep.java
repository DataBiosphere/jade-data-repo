package bio.terra.service.dataset.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetMetadataStep implements Step {
  private DatasetDao datasetDao;
  private UUID datasetId;
  private AuthenticatedUserRequest userReq;

  public DeleteDatasetMetadataStep(
      DatasetDao datasetDao, UUID datasetId, AuthenticatedUserRequest userReq) {
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    boolean success = datasetDao.delete(datasetId);
    DeleteResponseModel.ObjectStateEnum stateEnum =
        (success)
            ? DeleteResponseModel.ObjectStateEnum.DELETED
            : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // no undo is possible
    return StepResult.getStepResultSuccess();
  }
}
