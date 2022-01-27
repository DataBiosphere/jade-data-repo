package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.flight.delete.DeleteSnapshotMarkProjectStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDatasetMarkProjectStep implements Step {

  private final ResourceService resourceService;
  private final UUID datasetId;
  private final DatasetService datasetService;

  public DeleteDatasetMarkProjectStep(ResourceService resourceService, UUID datasetId, DatasetService datasetService) {
    this.resourceService = resourceService;
    this.datasetId = datasetId;
    this.datasetService = datasetService;
  }

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotMarkProjectStep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> projectIdList = workingMap.get(DatasetWorkingMapKeys.DATASET_PROJECT_ID_LIST, List.class);
    resourceService.markProjectsForDelete(projectIdList);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
