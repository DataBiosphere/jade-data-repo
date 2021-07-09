package bio.terra.datarepo.service.dataset.flight.create;

import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.dataset.DatasetDao;
import bio.terra.datarepo.service.dataset.DatasetJsonConversion;
import bio.terra.datarepo.service.dataset.DatasetUtils;
import bio.terra.datarepo.service.dataset.exception.InvalidDatasetException;
import bio.terra.datarepo.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetMetadataStep implements Step {

  private DatasetDao datasetDao;
  private DatasetRequestModel datasetRequest;

  private static Logger logger = LoggerFactory.getLogger(CreateDatasetMetadataStep.class);

  public CreateDatasetMetadataStep(DatasetDao datasetDao, DatasetRequestModel datasetRequest) {
    this.datasetDao = datasetDao;
    this.datasetRequest = datasetRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    try {
      FlightMap workingMap = context.getWorkingMap();
      UUID projectResourceId =
          workingMap.get(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, UUID.class);
      UUID applicationDeploymentResourceId =
          workingMap.get(DatasetWorkingMapKeys.APPLICATION_DEPLOYMENT_RESOURCE_ID, UUID.class);
      UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
      Dataset newDataset =
          DatasetUtils.convertRequestWithGeneratedNames(datasetRequest)
              .projectResourceId(projectResourceId)
              .applicationDeploymentResourceId(applicationDeploymentResourceId)
              .id(datasetId);
      datasetDao.createAndLock(newDataset, context.getFlightId());

      DatasetSummaryModel datasetSummary =
          DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(
              newDataset.getDatasetSummary());
      workingMap.put(JobMapKeys.RESPONSE.getKeyName(), datasetSummary);
      return StepResult.getStepResultSuccess();
    } catch (InvalidDatasetException idEx) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, idEx);
    } catch (Exception ex) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new InvalidDatasetException("Cannot create dataset: " + datasetRequest.getName(), ex));
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    logger.debug("Dataset creation failed. Deleting metadata.");
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    datasetDao.delete(datasetId);
    return StepResult.getStepResultSuccess();
  }
}
