package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class DeleteDatasetDeleteProjectStep implements Step {
  private final ResourceService resourceService;

  public DeleteDatasetDeleteProjectStep(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> projectIdList =
        workingMap.get(DatasetWorkingMapKeys.DATASET_PROJECT_ID_LIST, List.class);
    resourceService.deleteUnusedProjects(projectIdList);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
