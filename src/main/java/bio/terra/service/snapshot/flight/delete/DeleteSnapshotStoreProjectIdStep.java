package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class DeleteSnapshotStoreProjectIdStep extends DefaultUndoStep {
  private final UUID snapshotId;
  private final SnapshotService snapshotService;

  public DeleteSnapshotStoreProjectIdStep(UUID snapshotId, SnapshotService snapshotService) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    UUID projectResourceId = snapshot.getProjectResourceId();

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);

    return StepResult.getStepResultSuccess();
  }
}
