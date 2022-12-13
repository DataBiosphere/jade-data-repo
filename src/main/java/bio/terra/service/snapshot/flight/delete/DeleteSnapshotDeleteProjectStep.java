package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class DeleteSnapshotDeleteProjectStep extends DefaultUndoStep {

  private final ResourceService resourceService;

  public DeleteSnapshotDeleteProjectStep(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> projectIds =
        workingMap.get(SnapshotWorkingMapKeys.PROJECTS_MARKED_FOR_DELETE, List.class);
    resourceService.deleteUnusedProjects(projectIds);
    return StepResult.getStepResultSuccess();
  }
}
