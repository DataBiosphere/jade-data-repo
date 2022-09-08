package bio.terra.service.dataset.flight.update;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DatasetSchemaUpdateResponseStep implements Step {
  private final DatasetDao datasetDao;
  private final UUID datasetId;
  private final DatasetService datasetService;
  private final DatasetSchemaUpdateModel updateModel;
  private final AuthenticatedUserRequest userRequest;

  public DatasetSchemaUpdateResponseStep(
      DatasetDao datasetDao,
      UUID datasetId,
      DatasetService datasetService,
      DatasetSchemaUpdateModel updateModel,
      AuthenticatedUserRequest userRequest) {
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.updateModel = updateModel;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetDao.retrieve(datasetId);
    DatasetModel updatedDataset =
        datasetService.retrieveModel(
            dataset, userRequest, List.of(DatasetRequestAccessIncludeModel.SCHEMA));
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), updatedDataset);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
