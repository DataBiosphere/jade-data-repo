package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.TransactionSystemException;

public class DeleteSnapshotMarkProjectStep implements Step {

  private final ResourceService resourceService;
  private final UUID snapshotId;
  private final SnapshotService snapshotService;

  public DeleteSnapshotMarkProjectStep(
      ResourceService resourceService, UUID snapshotId, SnapshotService snapshotService) {
    this.resourceService = resourceService;
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID projectId = workingMap.get(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, UUID.class);

    try {
      List<UUID> projectsToBeDeleted =
          resourceService.markUnusedProjectsForDelete(List.of(projectId));

      workingMap.put(SnapshotWorkingMapKeys.PROJECTS_MARKED_FOR_DELETE, projectsToBeDeleted);

      return StepResult.getStepResultSuccess();
    } catch (TransientDataAccessException | TransactionSystemException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
