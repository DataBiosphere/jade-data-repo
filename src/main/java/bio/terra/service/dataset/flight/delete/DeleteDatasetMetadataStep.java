package bio.terra.service.dataset.flight.delete;

import bio.terra.common.BaseStep;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetMetadataStep extends BaseStep {
  private DatasetDao datasetDao;
  private UUID datasetId;

  public DeleteDatasetMetadataStep(DatasetDao datasetDao, UUID datasetId) {
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult perform() {
    boolean success = datasetDao.delete(datasetId);
    DeleteResponseModel.ObjectStateEnum stateEnum =
        (success)
            ? DeleteResponseModel.ObjectStateEnum.DELETED
            : DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
    DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
    setResponse(deleteResponseModel, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }
}
