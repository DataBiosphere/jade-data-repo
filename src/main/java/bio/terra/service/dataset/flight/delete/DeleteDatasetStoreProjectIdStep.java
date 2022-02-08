package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class DeleteDatasetStoreProjectIdStep implements Step {
  private final UUID datasetId;
  private final DatasetService datasetService;

  public DeleteDatasetStoreProjectIdStep(UUID datasetId, DatasetService datasetService) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    UUID projectResourceId = dataset.getProjectResourceId();

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
